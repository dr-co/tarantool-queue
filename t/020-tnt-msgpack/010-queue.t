#!/usr/bin/perl

use warnings;
use strict;
use utf8;
use open qw(:std :utf8);
use lib qw(lib ../lib);

use Test::More tests    => 25;
use Encode qw(decode encode);


BEGIN {
    unless (eval 'require DR::Tnt') {
        plan skip_all => 'DR::Tnt is not installed';
    }

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
    }
);

ok $q->tnt->ping, 'ping';

for ('put', 'urgent') {
    my $task1 = $q->$_;
    is_deeply $task1->data, undef, "$_()";
    like $task1->id, qr[^\d+$], 'task1.id';
    my $task2 = $q->$_(data => { 1 => 2 });
    like $task2->id, qr[^\d+$], 'task2.id';
    is_deeply $task2->data, { 1 => 2 }, "$_(data => hashref)";
    my $task3 = $q->$_(data => [ 3, 4, 'привет' ]);
    like $task3->id, qr[^\d+$], 'task3.id';
    is_deeply $task3->data, [ 3, 4, 'привет' ], "$_(data => arrayref)";
}

my $task1_t = $q->take;
isa_ok $task1_t => 'DR::TarantoolQueue::Task';
my $task2_t = $q->take;
isa_ok $task2_t => 'DR::TarantoolQueue::Task';
my $task3_t = $q->take;
isa_ok $task3_t => 'DR::TarantoolQueue::Task';

isnt $task1_t->id, $task2_t->id, "task1 and task2 aren't the same";
isnt $task1_t->id, $task3_t->id, "task1 and task3 aren't the same";

is $task1_t->status, 'work', 'task is taken';
isa_ok $task1_t->ack => 'DR::TarantoolQueue::Task', 'task1.ack';
is $task1_t->status, 'ack(removed)', 'task is ack';
isa_ok $q->ack(id => $task2_t->id), 'DR::TarantoolQueue::Task', 'task2.ack';

