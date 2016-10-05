local log = require 'log'
local fiber = require 'fiber'


local ID            = 1
local TUBE          = 2
local PRI           = 3
local DOMAIN        = 4
local STATUS        = 5
local EVENT         = 6
local CLIENT        = 7
local OPTIONS       = 8
local DATA          = 9


local C_ID          = 1
local C_TUBE        = 2
local C_CLIENT      = 3
local C_FID         = 4


_G.mq = {
    VERSION                 = '1.0',

    TIMEOUT_INFINITY        = 86400 * 365 * 100,

    defaults    = {
        ttl             = 86400,
        ttr             = 86400,
        pri             = 0,
        domain          = '',
        delay           = 0,
    },




    -- last serials
    serial  = {
        MegaQueue               = nil,
        MegaQueueConsumers      = nil,
    },


    migrations  = require('megaqueue.migrations')
}

function mq.extend(self, t1, t2)
    local res = {}

    if t1 ~= nil then
        for k, v in pairs(t1) do
            res[k] = v
        end
    end

    if t2 ~= nil then
        for k, v in pairs(t2) do
            res[k] = v
        end
    end

    return res
end


function mq.init(self)
    local upgrades = self.migrations:upgrade(self)
    log.info('MegaQueue started')
    return upgrades
end

function mq._serial(self, space)
    if self.serial[space] == nil then
        local max = box.space[space].index.id:max()
        if max ~= nil then
            self.serial[space] = max[1]
        else
            self.serial[space] = tonumber64(30000000000)
        end
    end
    return self.serial[space] + tonumber64(1)
end

function mq._next_serial(self, space)
    self.serial[space] = self:_serial(space)
end

function mq._mktuple(self, tube, status, opts, data)


    return tuple
end


function mq._take(self, tube)
    local task = box.space.MegaQueue.index.tube_status_pri_id
                                :min{ tube, 'ready' }

    if task == nil or task[STATUS] ~= 'ready' then
        return self:_normalize()
    end

    box.begin()

        task = box.space.MegaQueue:update(
                task[ID], {
                    { '=', STATUS, 'work' },
                    { '=', CLIENT, box.session.id() }
                })
        -- TODO: update statistics
    box.commit()

    -- wakeup
    return self:_normalize(task)
end

function mq._normalize(self, task)
    if task == nil then
        return
    end
    return task
end

function mq._process_tube(self, tube)

end

-------------------------------------------------------------------------------
-- Public API
-------------------------------------------------------------------------------
function mq.put(self, tube, opts, data)
    opts = self:extend(self.defaults, opts)

    -- perl or some the othe langs can't recognize 1 and '1'
    opts.domain = tostring(opts.domain)
    tube = tostring(tube)

    local status = 'ready'
    if opts.delay > 0 then
        opts.ttl = opts.ttl + opts.delay
        status = 'delayed'
    end

    -- checks domain
    if status == 'ready' and opts.domain ~= '' then
        local exists = box.space.MegaQueue.index.tube_domain_status
                        :select({tube, opts.domain, 'ready'},
                            { iterator = 'EQ', limit = 1 })
        if #exists == 0 then
            exists = box.space.MegaQueue.index.tube_domain_status
                        :select({tube, opts.domain, 'work'},
                            { iterator = 'EQ', limit = 1 })
        end

        if #exists > 0 then
            status = 'wait'
        end
    end

    local event

    opts.created = fiber.time()

    if status == 'delayed' then
        event = opts.created + opts.delay
    else
        event = opts.created + opts.ttl
    end

    local pri = opts.pri
    local domain = opts.domain
    opts.pri = nil
    opts.domain = nil

    local tuple = box.tuple.new {
        [ID]        = self:_serial('MegaQueue'),
        [TUBE]      = tube,
        [PRI]       = pri,
        [DOMAIN]    = domain,
        [STATUS]    = status,
        [EVENT]     = event,
        [CLIENT]    = 0,
        [OPTIONS]   = opts,
        [DATA]      = data,
    }

    box.begin()
        tuple = box.space.MegaQueue:insert(tuple)
        self:_next_serial('MegaQueue')
        -- TODO: update statistic
    box.commit()

    self:_process_tube(tube)

    return self:_normalize(tuple)
end

function mq.take(self, tube, timeout)
    if timeout == nil then
        timeout = self.TIMEOUT_INFINITY
    else
        timeout = tonumber(timeout)
    end

    tube = tostring(tube)

    local started = fiber.time()

    while timeout >= 0 do

        local task = self:_take(tube)
        if task ~= nil then
            return self:_normalize(task)
        end

        if timeout == 0 then
            return
        end

        local consumer = box.tuple.new{
            [C_ID]          = self:_serial('MegaQueueConsumers'),
            [C_TUBE]        = tube,
            [C_CLIENT]      = box.session.id(),
            [C_FID]         = fiber.id(),
        }

        box.begin()
            box.space.MegaQueueConsumers:insert(consumer)
            self:_next_serial('MegaQueueConsumers')
        box.commit()

        fiber.sleep(timeout)

        box.space.MegaQueueConsumers:delete(consumer[C_ID])

        local now = fiber.time()
        timeout = timeout - (now - started)
        started = now

        if timeout < 0 then
            return self:_take(tube)
        end
    end
end

-- each tarantool's start upgrade queue
mq:init()

return mq
