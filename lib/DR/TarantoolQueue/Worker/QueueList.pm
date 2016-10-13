use utf8;
use strict;
use warnings;
package DR::TarantoolQueue::Worker::QueueList;
use Mouse::Role;

use Mouse::Util::TypeConstraints;

subtype QueueList => as 'ArrayRef[DR::TarantoolQueue]';

coerce QueueList => from 'DR::TarantoolQueue', via { [ $_ ] };
coerce QueueList => from 'Undef', via { [] };

no Mouse::Util::TypeConstraints;

has queue  =>
    isa         => 'QueueList',
    is          => 'ro',
    required    => 1,
    coerce      => 1;


1
;
