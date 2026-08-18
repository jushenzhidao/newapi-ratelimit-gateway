-- token_deduct.lua
-- Token 模式专用：请求完成后扣减 token 用量
--
-- KEYS[1] = user_id (sha256(api_key) 或 group name)
-- ARGV[1] = 当前时间戳(秒)
-- ARGV[2] = 要扣减的 token 数量

local user_id = KEYS[1]
local now = tonumber(ARGV[1])
local tokens = tonumber(ARGV[2])

local windows = {18000, 604800, 2592000}

for i, ttl in ipairs(windows) do
    local key = "token_usage:" .. user_id .. ":" .. ttl
    local current = tonumber(redis.call("GET", key) or "0")
    local new_val = current + tokens
    redis.call("SET", key, new_val, "EX", ttl)
end

return 1
