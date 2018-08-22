#!/usr/bin/perl

use warnings;
use strict;
use utf8;
use open qw(:std :utf8);
use lib qw(lib ../lib);

use constant PLAN   => 27;
use Test::More;
use Encode qw(decode encode);


BEGIN {
    unless (eval 'require DR::Tnt') {
        plan skip_all => 'DR::Tnt is not installed';
    }
    plan tests    => PLAN;
    use_ok 'DR::TarantoolQueue';
    use_ok 'DR::Tnt::Test';
    tarantool_version_check(1.6);
}


my $t = start_tarantool
    -port   => free_port,
    -lua    => 't/020-tnt-msgpack/lua/queue.lua',
;

diag $t->log unless ok $t->is_started, 'Queue was started';


my $q = DR::TarantoolQueue->new(
    host        => '127.0.0.1',
    port        => $t->port,
    user        => 'test',
    password    => 'test',
    msgpack     => 1,
    coro        => 0,

    tube        => 'test_tube',

    ttl         => 60,

    defaults    => {
        test_tube   => {
            ttl         => 80
        }
    },
    fake_in_test    => 0,
);

ok $q->tnt->ping, 'ping';
is $q->_fake_msgpack_tnt, undef, 'Do not use fake queue';

subtest 'put inspector first' => sub {
    
    plan tests => 8;
    my $t1 = $q->put(tube => 't', data => 1, domain => 'a', inspect => 1);
    isa_ok $t1 => 'DR::TarantoolQueue::Task', 'put task1';
    is $t1->status, 'ready', 'status';
    is $t1->options->{inspect}, 1, 'inspect';
    isnt $t1->options->{data}, 1, 'options.data';

    my $t1t = $q->take(tube => 't', timeout => .1);
    isa_ok $t1t => 'DR::TarantoolQueue::Task', 'taken task1';
    is $t1t->id, $t1->id, 'id';
    is $t1t->status, 'work', 'status';
    ok $t1t->ack, 'ack';
};
    
my $t1 = $q->put(tube => 't', data => 1, domain => 'a');
isa_ok $t1 => 'DR::TarantoolQueue::Task', 'put task1';
is $t1->status, 'ready', 'status';
is $t1->options->{inspect}, undef, 'inspect';

my $ti = $q->put(tube => 't', data => 'x', domain => 'a', inspect => 1);
isa_ok $ti => 'DR::TarantoolQueue::Task', 'put task1';
is $ti->status, 'inspect', 'status';
is $ti->options->{inspect}, 1, 'inspect';

my $t2 = $q->put(tube => 't', data => 2, domain => 'a');
isa_ok $t2 => 'DR::TarantoolQueue::Task', 'put task1';
is $t2->status, 'wait', 'status';
is $t2->options->{inspect}, undef, 'inspect';
    
my $t1t = $q->take(tube => 't', timeout => .1);
isa_ok $t1t => 'DR::TarantoolQueue::Task', 'taken task1';
is $t1t->id, $t1->id, 'id';
is $t1t->status, 'work', 'status';
ok $t1t->ack, 'ack';

my $t2t = $q->take(tube => 't', timeout => .1);
isa_ok $t2t => 'DR::TarantoolQueue::Task', 'taken task2';
is $t2t->id, $t2->id, 'id';
is $t2t->status, 'work', 'status';
ok $t2t->ack, 'ack';

my $tit = $q->take(tube => 't', timeout => .1);
isa_ok $tit => 'DR::TarantoolQueue::Task', 'taken task inspector';
is $tit->id, $ti->id, 'id';
is $tit->status, 'work', 'status';
ok $tit->ack, 'ack';
