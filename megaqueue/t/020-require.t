#!/usr/bin/env tarantool

local yaml = require 'yaml'
local test = require('tap').test()
test:plan(4)

local tnt = require('t.tnt')
test:ok(tnt, 'tarantool loaded')
tnt.cfg{}

local mq = require 'megaqueue'
test:like(tnt.log(), 'First start of megaqueue', 'upgrade process started')

test:is(mq:init(), 0, 'Reinit does nothing')

test:ok(box.space.MegaQueue, 'Space created')


-- print(tnt.log())
-- print(yaml.encode(box.space._space.index.name:select('MegaQueue')))

tnt.finish()
os.exit(test:check() == true and 0 or -1)

