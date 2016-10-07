#!/usr/bin/env tarantool

local json = require 'json'
local test = require('tap').test()
local fiber = require 'fiber'
test:plan(9)

local tnt = require('t.tnt')
test:ok(tnt, 'tarantool loaded')
tnt.cfg{}

local mq = require 'megaqueue'
test:ok(mq, 'queue loaded')
test:ok(mq:init() > 0, 'First init queue')

test:ok(box.space.MegaQueue, 'Space created')



local task = mq:put('tube1', { ttl = 1 }, 123)
test:ok(task ~= nil, 'task was put')

local taken = mq:take('tube1', 0.01)
test:ok(taken ~= nil, 'task was taken')

test:diag(json.encode(taken))

local ack = mq:ack(taken)
test:ok(ack ~= nil, 'task was acked')

test:is(ack[5], 'removed', 'task status is removed')

local db = box.space.MegaQueue:get(task[1])
test:ok(db == nil, 'DB do not contain the task')





-- print(tnt.log())
-- print(yaml.encode(box.space._space.index.name:select('MegaQueue')))

tnt.finish()
os.exit(test:check() == true and 0 or -1)



