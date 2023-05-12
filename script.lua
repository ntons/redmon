-- 二分查找索引k，使得 f(a[1,...,k-1]) == false && f(a[k,...#a]) == true
local function binarysearch(a, f)
    local i, j = 1, #a + 1
    while i < j do
        local k = math.floor((i + j) / 2)
        if f(a[k]) then j = k else i = k + 1 end
    end
    return i
end

-- 已回写数据的默认过期时长
local DEFAULT_EX = 86400

-- 脏数据标记，脏KEY先进SET，如果SET中之前没有再进QUE，保证QUE中KEY的唯一性
local DIRTY_SET = "$DIRTYSET$"
local DIRTY_QUE = "$DIRTYQUE$"

-- 保存数据，同时标记脏KEY
local function remon_save(k, d, v)
    d.rev = d.rev + 1
    if v then d.val = v end
    local b = cmsgpack.pack(d)
    redis.call("SET", k, b)
    if redis.call("SADD", DIRTY_SET, k) > 0 then
        redis.call("LPUSH", DIRTY_QUE, k)
    end
    return b
end

-- 加载数据
-- ARGV[1] 数据
-- ARGV[2] 过期时长，默认: 86400
-- RET 0
local function remon_load()
    local b = redis.call("GET", KEYS[1])
    if not b or cmsgpack.unpack(b).rev < cmsgpack.unpack(ARGV[1]).rev then
        redis.call("SET", KEYS[1], ARGV[1], "EX", tonumber(ARGV[2] or DEFAULT_EX))
    end
    return 0
end

-- 获取数据，要处理CreateIfNotExist语义，所以必须用脚本
-- ARGV[1] 如果数据不存在用此值创建(CreateIfNotExist)
-- RET nil未加载数据 or 当前数据
local function remon_get()
    local b = redis.call("GET", KEYS[1])
    if not b then return nil end
    if ARGV[1] then
        local d = cmsgpack.unpack(b)
        if d.rev == 0 then
            b = remon_save(KEYS[1], d, ARGV[1])
        end
    end
    return b
end

-- 修改数据，不管存在与否
-- ARGV[1] 数据
-- RET nil为加载数据 or 当前修订
local function remon_set()
    local b = redis.call("GET", KEYS[1])
    if not b then return nil end
    local d = cmsgpack.unpack(b)
    remon_save(KEYS[1], d, ARGV[1])
    return d.rev
end

-- 创建数据
-- ARGV[1] 数据
-- RET nil未加载数据 or 0数据已存在 or 1创建成功
local function remon_add()
    local b = redis.call("GET",KEYS[1])
    if not b then return nil end
    local d = cmsgpack.unpack(b)
    if d.rev ~= 0 then return 0 end
    remon_save(KEYS[1], d, ARGV[1])
    return d.rev
end

-- 包装邮箱处理方法
-- ARGV 由实际处理方法定义
-- RET nil数据未加载 or 实际处理方法返回
local function remon_mb_call(f)
    local b = redis.call("GET", KEYS[1])
    if not b then return nil end
    local d = cmsgpack.unpack(b)
    local mb = { seq=0, que={} }
    if #d.val > 0 then mb = cmsgpack.unpack(d.val) end
    local r = f(mb)
    remon_save(KEYS[1], d, cmsgpack.pack(mb))
    return r
end

-- 推送邮件
-- ARGV[1] 邮件数据
-- ARGV[2] 邮件重要度
-- ARGV[3] 邮箱容量
-- ARGV[4] 淘汰策略，目前只有1，删除最不重要且最早的，淘汰发生在插入后，当前插入的邮件可能被立即淘汰掉
-- RET 进入队列的邮件ID
local function remon_mb_push(mb)
    -- 插入
    mb.seq = mb.seq + 1
    local id = tonumber(ARGV[2] or 0) * 1e10 + mb.seq
    local i = binarysearch(mb.que, function(m) return m.id > id end)
    table.insert(mb.que, i, { id=id, val=ARGV[1] })
    -- 淘汰
    local cap = tonumber(ARGV[3] or 0)
    if not cap then error("bad capacity") end
    if cap > 0 and #mb.que > cap then
        if tonumber(ARGV[4] or 0) == 1 then
            while #mb.que > cap do table.remove(mb.que, 1) end
        else
            return -1
        end
    end
    return id
end

-- 删除邮件
-- ARGV 待删除邮件ID列表
-- RET 被成功删除的邮件ID列表(其他的ID不存在)
local function remon_mb_pull(mb)
    local r = {}
    for _, v in ipairs(ARGV) do
        local id = tonumber(v)
        local i = binarysearch(mb.que, function(m) return m.id >= id end)
        if i <= #mb.que and mb.que[i].id == id then
            r[#r+1] = mb.que[i].id
            table.remove(mb.que, i)
        end
    end
    return r
end

-- 回写数据
-- KEYS[1] 可选，已回写键值
-- ARGV[1] 可选，已回写修订
-- ARGV[2] 过期时长，默认: 86400
-- RET {待回写键值，待回写数据}
local function remon_sync()
    assert(#KEYS < 2 and #KEYS == #ARGV)
    if #KEYS > 0 and redis.call("LINDEX", DIRTY_QUE, -1) == KEYS[1] then
        local b = redis.call("GET", KEYS[1])
        if not b then
            redis.call("RPOP", DIRTY_QUE)
            redis.call("SREM", DIRTY_SET, KEYS[1])
        else
            local d = cmsgpack.unpack(b)
            if tostring(d.rev) == ARGV[1] then
                redis.call("RPOP", DIRTY_QUE)
                redis.call("SREM", DIRTY_SET, KEYS[1])
                redis.call("EXPIRE", KEYS[1], tonumber(ARGV[2] or DEFAULT_EX))
            else
                redis.call("RPOPLPUSH", DIRTY_QUE, DIRTY_QUE)
            end
        end
    end
    local k = redis.call("LINDEX", DIRTY_QUE, -1)
    if not k then return nil end
    local b = redis.call("GET", k)
    if not b then
        redis.call("RPOP", DIRTY_QUE)
        redis.call("SREM", DIRTY_SET, k)
        return nil
    end
    return {k, b}
end

local cmd = ARGV[1]
table.remove(ARGV, 1)
if cmd == "remon_load" then
    return remon_load()
elseif cmd == "remon_get" then
    return remon_get()
elseif cmd == "remon_set" then
    return remon_set()
elseif cmd == "remon_add" then
    return remon_add()
elseif cmd == "remon_mb_push" then
    return remon_mb_call(remon_mb_push)
elseif cmd == "remon_mb_pull" then
    return remon_mb_call(remon_mb_pull)
elseif cmd == "remon_sync" then
    return remon_sync()
else
    error("remon: bad command")
end
