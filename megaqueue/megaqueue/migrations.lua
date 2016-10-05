local SCH_KEY       = 'MegaQueueDbVersion'

local log = require 'log'
local migrations = {}
migrations.list = {
    {
        up  = function()
            log.info('First start of megaqueue detected')
        end
    },
    {
        description = 'Create main MegaQueue space',
        up  = function()
            box.schema.space.create(
                'MegaQueue',
                {
                    engine      = 'memtx',
                    format  = {
                        {                           -- #1
                            ['name']    = 'id',
                            ['type']    = 'num',
                        },

                        {                           -- #2
                            ['name']    = 'tube',
                            ['type']    = 'str',
                        },

                        {                           -- #3
                            ['name']    = 'pri',
                            ['type']    = 'num',
                        },

                        {                           -- #4
                            ['name']    = 'domain',
                            ['type']    = 'str',
                        },

                        {                           -- #5
                            ['name']    = 'status',
                            ['type']    = 'str',
                        },

                        {                           -- #6
                            ['name']    = 'event',
                            ['type']    = 'num',
                        },

                        {                           -- #7
                            ['name']    = 'client',
                            ['type']    = 'num',
                        },

                        {                           -- #8
                            ['name']    = 'options',
                            ['type']    = '*',
                        },
                        
                        {                           -- #9
                            ['name']    = 'data',
                            ['type']    = '*',
                        },
                    }
                }
            )
        end
    },

    {
        description = 'MegaQueue: main space primary index',
        up = function()
            box.space.MegaQueue:create_index(
                'id',
                {
                    unique  = true,
                    type    = 'tree',
                    parts   = { 1, 'num' }
                }
            )
        end
    },

    {
        description = 'MegaQueue: create domain index',
        up  = function()
            box.space.MegaQueue:create_index(
                'tube_domain_status',
                {
                    unique  = false,
                    type    = 'tree',
                    parts   = { 2, 'str', 4, 'str', 5, 'str' }
                }
            )
        end
    },

    {
        description = 'MegaQueue: work index',
        up = function()
            box.space.MegaQueue:create_index(
                'tube_status_pri_id',
                {
                    unique  = false,
                    type    = 'tree',
                    parts   = { 2, 'str', 5, 'str', 3, 'num', 1, 'num' }
                }
            )
        end
    },

    {
        description = 'Create space MegaQueueConsumers',
        up  = function()
            box.schema.space.create(
                'MegaQueueConsumers',
                {
                    engine      = 'memtx',
                    format  = {
                        {                               -- #1
                            ['name']    = 'id',
                            ['type']    = 'num',
                        },
                        {                               -- #2
                            ['name']    = 'tube',
                            ['type']    = 'str',
                        },
                        {                               -- #3
                            ['name']    = 'session',
                            ['type']    = 'num',
                        },
                        {                               -- #4
                            ['name']    = 'fid',
                            ['type']    = 'num',
                        }
                    },
                    temporary   = true,
                }
            )
        end
    },

    {
        description = 'Create primary index MegaQueueConsumers',
        up = function()
            box.space.MegaQueueConsumers:create_index(
                'id',
                {
                    unique  = true,
                    type    = 'tree',
                    parts   = { 1, 'num' }
                }
            )
        end
    },
    
    {
        description = 'Create tube index MegaQueueConsumers',
        up = function()
            box.space.MegaQueueConsumers:create_index(
                'tube_session_id',
                {
                    unique  = true,
                    type    = 'tree',
                    parts   = { 2, 'str',  3, 'num', 1, 'num' }
                }
            )
        end
    },
}


function migrations.upgrade(self, mq)

    local db_version = 0
    local ut = box.space._schema:get(SCH_KEY)
    local version = mq.VERSION

    if ut ~= nil then
        db_version = ut[2]
    end

    local cnt = 0
    for v, m in pairs(migrations.list) do
        if db_version < v then
            local nv = string.format('%s.%03d', version, v)
            log.info('MegaQueue: up to version %s (%s)', nv, m.description)
            m.up(mq)
            box.space._schema:replace{ SCH_KEY, v }
            mq.VERSION = nv
            cnt = cnt + 1
        end
    end
    return cnt
end


return migrations
