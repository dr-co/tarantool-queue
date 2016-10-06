#!/usr/bin/env tarantool

local yaml = require 'yaml'
local test = require('tap').test()
local fiber = require 'fiber'
test:plan(6)

local tnt = require('t.tnt')
test:ok(tnt, 'tarantool loaded')
tnt.cfg{}

local mq = require 'megaqueue'
test:ok(mq, 'queue loaded')
test:ok(mq:init() > 0, 'First init queue')

test:ok(box.space.MegaQueue, 'Space created')

local started = fiber.time()
test:test('put delayed task', function(test)
    test:plan(2)
    local task = mq:put('tube1', { delay = .25 }, 123)
    test:ok(task ~= nil, 'task was put')
    test:is(task[5], 'delayed', 'state')
end)

test:test("take ready task", function(test)

    test:plan(11)


    local task = mq:take('tube1', 0.5)
    test:ok(task, 'task was taken')

    test:is(task[2], 'tube1', 'tube name')
    test:is(task[3], mq.defaults.pri, 'task pri')
    test:is(task[4], '', 'task domain')
    test:is(task[5], 'work', 'task status')
    test:ok(task[6] <= fiber.time() + mq.defaults.ttl, 'next event at ttl')
    test:is(task[7], box.session.id(), 'task client id')
    test:ok(task[8].created <= fiber.time(), 'task created')
    test:is(task[9], 123, 'task data')
    test:ok(fiber.time() - started >= 0.25, 'waiting time')

    task = mq:take('tube1', 0.1)
    test:ok(task == nil, 'second task is not taken')
end)



-- test:diag(tnt.log())
----------------------------------------------------
tnt.finish()
os.exit(test:check() == true and 0 or -1)



