-- rate_limit.lua
-- 全合并限速脚本：Key->分组->策略->检查->计数，单次 Redis 往返
--
-- KEYS[1] = sha256(api_key)
-- ARGV[1] = 当前时间戳(秒)
-- ARGV[2] = key_map_ttl (秒)
-- ARGV[3] = config_cache_ttl (秒)
--
-- 返回值:
--   {1}                        = 放行
--   {0, "5h", group, limit}    = 5h窗口超限
--   {0, "7d", group, limit}    = 7d窗口超限
--   {0, "30d", group, limit}   = 30d窗口超限
--   {-1}                       = key未找到
--   {-2}                       = 分组配置未找到

local key_hash = KEYS[1]
local now = tonumber(ARGV[1])
local key_map_ttl = tonumber(ARGV[2])
local config_ttl = tonumber(ARGV[3])

-- 1. Key -> 分组
local group = redis.call("GET", "keymap:" .. key_hash)
if not group then
    return {-1}
end

-- 2. 分组 -> 限速策略
local config_str = redis.call("GET", "config:" .. group)
if not config_str then
    return {-2}
end

local config = cjson.decode(config_str)
local limit_type = config["type"]
local scope = config["scope"]

-- 3. 确定限速主体 (user_id)
local user_id
if scope == "group" then
    user_id = group
else
    user_id = key_hash
end

-- 4. 检查 3 个滑动窗口
local windows = {
    {18000,   tonumber(config["5h"]),   "5h"},
    {604800,  tonumber(config["7d"]),   "7d"},
    {2592000, tonumber(config["30d"]),  "30d"}
}

if limit_type == "request" then
    -- 按请求数限速: 使用 ZSET 滑动窗口
    for i, w in ipairs(windows) do
        local ttl, limit, name = w[1], w[2], w[3]
        local key = "ratelimit:" .. user_id .. ":" .. ttl
        redis.call("ZREMRANGEBYSCORE", key, 0, now - ttl)
        local count = redis.call("ZCARD", key)
        if count >= limit then
            return {0, name, group, limit, count}
        end
    end

    -- 5. 全部通过，各窗口计数 +1
    for i, w in ipairs(windows) do
        local ttl = w[1]
        local key = "ratelimit:" .. user_id .. ":" .. ttl
        local member = now .. ":" .. math.random(100000000)
        redis.call("ZADD", key, now, member)
        redis.call("EXPIRE", key, ttl)
    end

    -- 返回剩余配额
    local remaining = {}
    for i, w in ipairs(windows) do
        local ttl, limit = w[1], w[2]
        local key = "ratelimit:" .. user_id .. ":" .. ttl
        local count = redis.call("ZCARD", key)
        remaining[i] = limit - count
    end

    return {1, group, remaining[1], remaining[2], remaining[3]}

else
    -- 按 Token 用量限速: 使用 String 累加器
    -- 此脚本只做检查，扣减由应用层在响应后调用 token_deduct.lua
    for i, w in ipairs(windows) do
        local ttl, limit, name = w[1], w[2], w[3]
        local key = "token_usage:" .. user_id .. ":" .. ttl
        local used = tonumber(redis.call("GET", key) or "0")
        if used >= limit then
            return {0, name, group, limit, used}
        end
    end

    -- 返回当前用量和剩余
    local remaining = {}
    for i, w in ipairs(windows) do
        local ttl, limit = w[1], w[2]
        local key = "token_usage:" .. user_id .. ":" .. ttl
        local used = tonumber(redis.call("GET", key) or "0")
        remaining[i] = limit - used
    end

    return {1, group, remaining[1], remaining[2], remaining[3]}
end
