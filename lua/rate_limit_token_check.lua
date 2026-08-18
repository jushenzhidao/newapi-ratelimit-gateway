-- rate_limit_token_check.lua
-- Token 模式限速检查（只读检查，扣减由 token_deduct.lua 在响应后执行）
--
-- KEYS[1] = user_id
-- ARGV[1] = 5h 限额
-- ARGV[2] = 7d 限额
-- ARGV[3] = 30d 限额
--
-- 返回值:
--   {1, rem5h, rem7d, rem30d}               = 放行
--   {0, "5h"|"7d"|"30d", limit, used}       = 对应窗口超限

local uid = KEYS[1]

local windows = {
    {18000,   tonumber(ARGV[1]), "5h"},
    {604800,  tonumber(ARGV[2]), "7d"},
    {2592000, tonumber(ARGV[3]), "30d"}
}

local remaining = {}

for i, w in ipairs(windows) do
    local ttl, limit, name = w[1], w[2], w[3]
    local key = "token_usage:" .. uid .. ":" .. ttl
    local used = tonumber(redis.call("GET", key) or "0")
    remaining[i] = limit - used
    if used >= limit then
        return {0, name, limit, used}
    end
end

return {1, remaining[1], remaining[2], remaining[3]}
