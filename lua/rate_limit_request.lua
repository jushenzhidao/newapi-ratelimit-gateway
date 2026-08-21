-- rate_limit_request.lua
-- 请求数模式限速：flush 本地计数 + 检查 + 批量预取配额，单次 Redis 往返
--
-- keymap/config 查找已挪到应用侧（L1 缓存 / MySQL 读透），本脚本只做计数。
--
-- KEYS[1] = user_id (scope=group 时为分组名，否则为 key_hash)
-- ARGV[1] = 当前时间戳(秒)
-- ARGV[2] = 批量预取大小 batch_size
-- ARGV[3] = 5h 限额
-- ARGV[4] = 7d 限额
-- ARGV[5] = 30d 限额
-- ARGV[6] = cjson 数组：本地累计尚未 flush 的请求时间戳
--
-- 返回值:
--   {1, reserve, rem5h, rem7d, rem30d}      = 放行，reserve=本次预取的配额数
--   {0, "5h"|"7d"|"30d", limit, used}       = 对应窗口超限

local uid = KEYS[1]
local now = tonumber(ARGV[1])
local batch = tonumber(ARGV[2])
local flush = cjson.decode(ARGV[6])

local windows = {
    {18000,   tonumber(ARGV[3]), "5h"},
    {604800,  tonumber(ARGV[4]), "7d"},
    {2592000, tonumber(ARGV[5]), "30d"}
}

-- ── Phase 1: flush 到所有窗口 ──
-- 必须先 flush 全部窗口，再做超限判断。
-- 否则 5h 超限时早返回，7d/30d 从未 flush → 跨窗口计数断裂。
for i, w in ipairs(windows) do
    local ttl = w[1]
    local key = "ratelimit:" .. uid .. ":" .. ttl
    redis.call("ZREMRANGEBYSCORE", key, 0, now - ttl)
    for _, ts in ipairs(flush) do
        redis.call("ZADD", key, ts, tostring(ts) .. ":" .. math.random(100000000))
    end
    if #flush > 0 then
        redis.call("EXPIRE", key, ttl)
    end
end

-- ── Phase 2: 检查所有窗口 ──
local avail = nil
local remaining = {}
local exceeded = nil

for i, w in ipairs(windows) do
    local ttl, limit, name = w[1], w[2], w[3]
    local key = "ratelimit:" .. uid .. ":" .. ttl
    local count = redis.call("ZCARD", key)
    remaining[i] = limit - count

    if count >= limit and exceeded == nil then
        exceeded = {0, name, limit, count}
    end
    local a = limit - count
    if avail == nil or a < avail then
        avail = a
    end
end

if exceeded then
    return exceeded
end

-- 预取配额不在 Redis 端占位（避免幻影计数），多 worker 并发预取的
-- 总超发上界约为 (workers-1) * batch_size，由应用侧文档说明。
local reserve = math.min(batch, avail)
return {1, reserve, remaining[1], remaining[2], remaining[3]}
