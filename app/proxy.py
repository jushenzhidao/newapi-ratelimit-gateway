"""反向代理 - 透明转发请求到 NewAPI，支持流式响应

针对 tool call / function call 的优化:
1. 流式模式下使用 buffer 累积完整 SSE 事件再解析 usage，避免 chunk 边界切割导致 usage 丢失
2. token 模式 + 流式请求时自动注入 stream_options.include_usage=true，确保上游返回 usage
3. 非流式响应照常从 JSON body 提取 usage，tool call 响应结构不影响
"""

import logging
import json
from typing import Optional
import httpx
from fastapi import Request, Response
from fastapi.responses import StreamingResponse, JSONResponse

from app.config import config
from app.ratelimit import rate_limiter, RateLimitResult

logger = logging.getLogger(__name__)

HOP_BY_HOP_HEADERS = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host", "content-length",
}

# 需要注入 stream_options 的路径（OpenAI 兼容 chat/completions）
CHAT_COMPLETION_PATHS = {"/v1/chat/completions", "/chat/completions"}


def _extract_api_key(request: Request) -> Optional[str]:
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:]
    return request.headers.get("api-key")


def _build_forward_headers(request: Request) -> dict:
    headers = {}
    for key, value in request.headers.items():
        if key.lower() not in HOP_BY_HOP_HEADERS:
            headers[key] = value
    return headers


def _is_stream_request(request: Request, body: bytes) -> bool:
    accept = request.headers.get("accept", "")
    if "text/event-stream" in accept:
        return True
    if body:
        try:
            data = json.loads(body)
            if data.get("stream") is True:
                return True
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass
    return False


def _inject_include_usage(
    body: bytes, path: str, need_inject: bool
) -> bytes:
    """在流式请求中注入 stream_options.include_usage=true

    仅当以下条件全部满足时才注入:
    1. 调用方标记需要注入 (token 模式)
    2. 路径是 chat/completions
    3. 请求体是合法 JSON
    4. stream=true
    5. 客户端未自行设置 stream_options.include_usage

    修改后更新 Content-Length 由下游 hop-by-hop 移除处理。
    """
    if not need_inject:
        return body
    if path not in CHAT_COMPLETION_PATHS:
        return body

    try:
        data = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return body

    if data.get("stream") is not True:
        return body

    stream_options = data.get("stream_options")
    if isinstance(stream_options, dict) and stream_options.get("include_usage") is True:
        return body  # 客户端已设置，不覆盖

    if stream_options is None:
        data["stream_options"] = {"include_usage": True}
    elif isinstance(stream_options, dict):
        stream_options["include_usage"] = True
    else:
        return body  # stream_options 类型异常，不改

    return json.dumps(data, ensure_ascii=False).encode("utf-8")


async def handle_proxy(request: Request) -> Response:
    """主代理处理函数"""

    api_key = _extract_api_key(request)
    if not api_key:
        return JSONResponse(
            status_code=401,
            content={"error": "unauthorized", "message": "missing api key"},
        )

    result: RateLimitResult = await rate_limiter.check(api_key)

    if not result.allowed:
        response_body = {
            "error": "rate_limit_exceeded",
            "message": f"Rate limit exceeded: {result.reason}",
        }
        if config.ratelimit.show_remaining and result.remaining:
            response_body["remaining"] = result.remaining
        if result.group:
            response_body["group"] = result.group
        return JSONResponse(
            status_code=429,
            content=response_body,
            headers={"Retry-After": "300"},
        )

    return await _forward_to_newapi(request, result)


async def _forward_to_newapi(request: Request, rl_result: RateLimitResult) -> Response:
    path = request.url.path
    query = request.url.query
    target_url = f"{config.newapi.base_url}{path}"
    if query:
        target_url += f"?{query}"

    headers = _build_forward_headers(request)
    body = await request.body()
    is_stream = _is_stream_request(request, body)

    # 判断是否需要追踪 token 用量
    need_usage_tracking = False
    if rl_result.group:
        group_config = await rate_limiter.get_group_config(rl_result.group)
        if group_config and group_config.get("type") == "token":
            need_usage_tracking = True

    # 流式 + token 模式时，注入 stream_options.include_usage
    if is_stream and need_usage_tracking and config.ratelimit.auto_inject_include_usage:
        original_len = len(body)
        body = _inject_include_usage(body, path, need_inject=True)
        if len(body) != original_len:
            logger.debug("Injected stream_options.include_usage for token tracking")

    if is_stream:
        return await _proxy_stream(
            request.method, target_url, headers, body, rl_result, need_usage_tracking
        )
    else:
        return await _proxy_normal(
            request.method, target_url, headers, body, rl_result, need_usage_tracking
        )


async def _proxy_normal(
    method: str, url: str, headers: dict, body: bytes,
    rl_result: RateLimitResult, track_usage: bool,
) -> Response:
    async with httpx.AsyncClient(timeout=config.newapi.timeout) as client:
        try:
            resp = await client.request(method, url, headers=headers, content=body)
        except httpx.RequestError as e:
            logger.error(f"NewAPI request failed: {e}")
            return JSONResponse(
                status_code=502,
                content={"error": "bad_gateway", "message": str(e)},
            )

    if track_usage and resp.status_code == 200:
        usage = _extract_usage_from_response(resp)
        if usage:
            api_key = headers.get("authorization", "").replace("Bearer ", "")
            group_config = await rate_limiter.get_group_config(rl_result.group)
            if group_config:
                user_id = (
                    rl_result.group
                    if group_config.get("scope") == "group"
                    else rate_limiter.hash_key(api_key)
                )
                await rate_limiter.deduct_tokens(user_id, usage)

    response_headers = dict(resp.headers)
    response_headers.pop("content-length", None)
    response_headers.pop("transfer-encoding", None)

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=response_headers,
        media_type=resp.headers.get("content-type"),
    )


async def _proxy_stream(
    method: str, url: str, headers: dict, body: bytes,
    rl_result: RateLimitResult, track_usage: bool,
) -> StreamingResponse:
    async def stream_generator():
        usage_tokens = 0
        api_key = headers.get("authorization", "").replace("Bearer ", "")
        # SSE buffer: 累积不完整的 chunk，按 \n\n 分割完整事件
        sse_buffer = ""

        async with httpx.AsyncClient(timeout=config.newapi.timeout) as client:
            async with client.stream(method, url, headers=headers, content=body) as resp:
                async for chunk in resp.aiter_bytes():
                    # 先透传，不阻塞客户端
                    yield chunk

                    if track_usage:
                        try:
                            sse_buffer += chunk.decode("utf-8", errors="ignore")
                            # 按 \n\n 分割完整 SSE 事件
                            while "\n\n" in sse_buffer:
                                event_str, sse_buffer = sse_buffer.split("\n\n", 1)
                                usage_tokens = _extract_usage_from_sse_event(
                                    event_str, usage_tokens
                                )
                        except Exception:
                            pass

                # 处理 buffer 中可能残留的最后一个事件
                if track_usage and sse_buffer.strip():
                    usage_tokens = _extract_usage_from_sse_event(sse_buffer, usage_tokens)

                if track_usage and usage_tokens > 0:
                    group_config = await rate_limiter.get_group_config(rl_result.group)
                    if group_config:
                        user_id = (
                            rl_result.group
                            if group_config.get("scope") == "group"
                            else rate_limiter.hash_key(api_key)
                        )
                        await rate_limiter.deduct_tokens(user_id, usage_tokens)

    return StreamingResponse(stream_generator(), media_type="text/event-stream")


def _extract_usage_from_sse_event(event_str: str, current_usage: int) -> int:
    """从单个完整 SSE 事件中提取 usage.total_tokens

    一个 SSE 事件可能包含多行:
        data: {"id":"xxx", "usage":{"total_tokens": 1234}}
        data: [DONE]

    只取最后一个包含 usage 的 data 行（OpenAI 在最终 chunk 返回 usage）。
    """
    max_usage = current_usage
    for line in event_str.split("\n"):
        line = line.strip()
        if not line.startswith("data:"):
            continue
        payload = line[5:].strip()
        if payload == "[DONE]" or not payload:
            continue
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            continue
        usage = data.get("usage")
        if usage and isinstance(usage, dict):
            total = usage.get("total_tokens", 0)
            if total > max_usage:
                max_usage = total
    return max_usage


def _extract_usage_from_response(resp: httpx.Response) -> Optional[int]:
    try:
        data = resp.json()
        usage = data.get("usage")
        if usage and "total_tokens" in usage:
            return usage["total_tokens"]
    except Exception:
        pass
    return None
