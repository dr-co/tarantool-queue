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

local TIMEOUT_INFINITY        = 86400 * 365 * 100

local mq = {
    VERSION                 = '1.0',

    defaults    = {
        ttl                 = 86400,
        ttr                 = 86400,
        pri                 = 0,
        domain              = '',
        delay               = 0,
    },

    -- last serials
    private = {
        serial  = {
            MegaQueue               = nil,
        },

        migrations  = require('megaqueue.migrations'),


        consumer    = {}
    }
}


function mq._consumer_add(self, tube)
    if self.private.consumer[tube] == nil then
        self.private.consumer[tube] = {}
    end
    table.insert(self.private.consumer[tube], fiber.id())
end

function mq._consumer_drop(self)
    local fid = fiber.id()
    if self.private.consumer[tube] == nil then
        return
    end
    for i, fiber_id in pairs(self.private.consumer[tube]) do
        if fid == fiber_id then
            table.remove(self.private.consumer[tube], i)
            break
        end
    end
end

function mq._consumer_wakeup(self, tube)
    if self.private.consumer[tube] == nil then
        return
    end
    for i, fid in pairs(self.private.consumer[tube]) do
        if fid ~= nil then
            table.remove(self.private.consumer[tube], i)
            fiber.find(fid):wakeup()
            break
        end
    end
end

function mq._consumer_sleep(self, tube, timeout)
    self:_consumer_add(tube)
    fiber.sleep(timeout)
    self:_consumer_drop()
end



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



function mq._serial(self, space)
    if self.private.serial[space] == nil then
        local max = box.space[space].index.id:max()
        if max ~= nil then
            self.private.serial[space] = max[1]
        else
            self.private.serial[space] = tonumber64(0)
        end
    end
    return self.private.serial[space] + tonumber64(1)
end

function mq._next_serial(self, space)
    self.private.serial[space] = self:_serial(space)
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

    local ttl = task[OPTIONS].created + task[OPTIONS].ttl
    local ttr = task[OPTIONS].ttr + fiber.time()

    if ttr > ttl then
        ttr = ttl
    end

    box.begin()

        task = box.space.MegaQueue:update(
                task[ID], {
                    { '=', STATUS, 'work' },
                    { '=', CLIENT, box.session.id() },
                    { '=', EVENT, ttr }
                })
        -- TODO: update statistics
    box.commit()

    return task
end

function mq._normalize(self, task)
    if task == nil then
        return
    end
    return task
end



function mq._task_by_tube_domain(self, tube, domain, statuses)
    local list

    for _, status in pairs(statuses) do
        list = box.space.MegaQueue.index.tube_domain_status
                    :select({ tube, domain, status },
                                { iterator = 'EQ', limit = 1 })
        if #list > 0 then
            return list[1]
        end
    end
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
    elseif opts.domain ~= '' then
        -- checks domain
        local exists =
            self:_task_by_tube_domain(tube, opts.domain, { 'ready', 'work' })

        if exists ~= nil then
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

    local task = box.tuple.new {
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

    local consumer
    box.begin()
        task = box.space.MegaQueue:insert(task)
        self:_next_serial('MegaQueue')

        -- TODO: update statistic

        self:_consumer_wakeup(tube)
    box.commit()

    self:_wakeup_consumer(consumer)

    self:_process_tube(task)

    return self:_normalize(task)
end

function mq._wakeup_consumer(self, consumer)
    if consumer == nil then
        return
    end

    fiber.find(consumer[C_FID]):wakeup()
end

function mq.take(self, tube, timeout)
    if timeout == nil then
        timeout = TIMEOUT_INFINITY
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

        if timeout <= 0 then
            return
        end

        self:_consumer_sleep(tube, timeout)

        local now = fiber.time()
        timeout = timeout - (now - started)
        started = now

        if timeout < 0 then
            return self:_normalize(self:_take(tube))
        end
    end
end

function mq.init(self)
    local upgrades = self.private.migrations:upgrade(self)
    log.info('MegaQueue started')

    if self._run_fiber ~= nil then
        self._run_fiber[1] = false
        self._run_fiber = nil
    end

    self._run_fiber = { true }
    self:run_worker()


    return upgrades
end


function mq.run_worker(self)
    local rw = self._run_fiber

    fiber.create(function()
        local now
        while rw[1] do
            now = fiber.time()
            local task = box.space.MegaQueue.index.event:min()
            if task == nil then
                rw[2] = fiber.id()
                fiber.sleep(3600)
                rw[2] = nil
            else

                if task[EVENT] > now then
                    rw[2] = fiber.id()
                    fiber.sleep(task[EVENT] - now)
                    rw[2] = nil

                else
                    -- ttl works in ANY status
                    if task[OPTIONS].ttl + task[OPTIONS].created <= now then
                        self:_task_delete(task, 'TTL')


                    -- ttr
                    elseif task[STATUS] == 'work' then
                        self:_task_to_ready(task)

                    -- delayed to ready
                    elseif task[STATUS] == 'delayed' then
                        self:_task_to_ready(task)
                    else
                        error(
                            string.format(
                                'Internal error: event on task [%s]',
                                    require('json').encode(task)
                            )
                        )
                    end
                end
            end
        end
    end)
end

function mq._enqueue_task_by(self, task)
    if task[STATUS] ~= 'ready' and task[STATUS] ~= 'work' then
        return
    end

    if task[DOMAIN] == '' then
        return
    end

    -- check if error (impossible, but...)
    local exists =
        self:_task_by_tube_domain(
            task[TUBE],
            task[DOMAIN],
            { 'ready', 'work' }
        )

    if exists ~= nil then
        return
    end

    local wait_task =
        self:_task_by_tube_domain(
            task[TUBE],
            task[DOMAIN],
            { 'wait' }
        )

    if wait_task == nil then
        return
    end

    box.space.MegaQueue:update(wait_task[ID],
        {
            { '=', STATUS, 'ready' },
            { '=', CLIENT, 0 },
            { '=', EVENT,
                    wait_task[OPTIONS].ttl + wait_task[OPTIONS].created }
        }
    )
end

function mq._task_delete(self, task, reason)
    box.begin()
        box.space.MegaQueue:delete(task[ID])

        self:_enqueue_task_by(task)


        -- TODO: statistics
    box.commit()
end

function mq._task_to_ready(self, task)

    local status = 'ready'
    local event = task[OPTIONS].created + task[OPTIONS].ttl

    if task[DOMAIN] ~= '' then
        local ck_statuses
        if task[STATUS] == 'work' then
            ck_statuses = { 'ready' }
        else
            ck_statuses = { 'ready', 'work' }
        end
        
        local exists =
            self:_task_by_tube_domain(
                task[TUBE],
                task[DOMAIN],
                ck_statuses
            )
        if exists ~= nil then
            status = 'wait'
        end
    end

    local consumer
    box.begin()
        box.space.MegaQueue:update(task[ID], {
            { '=', STATUS, status },
            { '=', EVENT, event },
            { '=', CLIENT, 0 }
        })
        
        self:_consumer_wakeup(task[TUBE])
        -- TODO: statistics
    box.commit()
end

function mq._process_tube(self, tube)
    if self._run_fiber == nil then
        return
    end
    if self._run_fiber[2] == nil then
        return
    end
    local fid = self._run_fiber[2]
    self._run_fiber[2] = nil
    fiber.find(fid):wakeup()
end

return mq
