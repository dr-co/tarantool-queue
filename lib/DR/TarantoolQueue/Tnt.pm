use utf8;
use strict;
use warnings;

package DR::TarantoolQueue::Tnt;
use Mouse::Role;

requires 'fake_in_test';

has _fake_msgpack_tnt =>
    is      => 'rw',
    isa     => 'Maybe[Object]',
    lazy    => 1,
    builder => sub {
        my ($self) = @_;
        return undef unless $self->fake_in_test;
        return undef unless $0 =~ /\.t$/;

        require DR::Tnt::Test;

        my $t = DR::Tnt::Test::start_tarantool(
            -port       => DR::Tnt::Test::free_port(),
            -make_lua   => q{
                require('log').info('Fake Queue starting')
                box.cfg{ listen  = os.getenv('PRIMARY_PORT') }
                box.schema.user.create('test', { password = 'test' })
                box.schema.user.grant('test', 'read,write,execute', 'universe')
                _G.queue = require('megaqueue')
                queue:init()
                require('log').info('Fake Queue started')
            }
        );

        unless ($t->is_started) {
            warn $t->log;
            die "Can't start fake tarantool\n";
        }
        return $t;
    };


has tnt =>
    is      => 'rw',
    isa     => 'Object',
    lazy    => 1,
    builder => sub {
        my ($self) = @_;
        return $self->_build_msgpack_tnt if $self->msgpack;
        return $self->_build_lts_tnt;
    }
;

sub _build_msgpack_tnt {
    my ($self) = @_;
    require DR::Tnt;

    my $driver = 'sync';
    $driver = 'coro' if $self->coro;


    my ($host, $port, $user, $password) =
        ($self->host, $self->port, $self->user, $self->password);

    # in test
    if ($self->_fake_msgpack_tnt) {
        $host = '127.0.0.1';
        $port = $self->_fake_msgpack_tnt->port;
        $user = 'test';
        $password = 'test';
    }

    return DR::Tnt::tarantool(
        host            => $host,
        port            => $port,
        user            => $user,
        password        => $password,
        raise_error     => 1,


        %{ $self->connect_opts },

        driver          => $driver,
        hashify_tuples  => 1,
        utf8            => 0,
    )
}

sub _build_lts_tnt {
    my ($self) = @_;
    require DR::Tarantool;
    unless ($self->coro) {
        if (DR::Tarantool->can('rsync_tarantool')) {
            return DR::Tarantool::rsync_tarantool(
                port => $self->port,
                host => $self->host,
                spaces => {},
                %{ $self->connect_opts }
            );
        } else {
            return DR::Tarantool::tarantool(
                port => $self->port,
                host => $self->host,
                spaces => {},
                %{ $self->connect_opts }
            );
        }
    }

    return DR::Tarantool::coro_tarantool(
        port => $self->port,
        host => $self->host,
        spaces => {},
        %{ $self->connect_opts }
    );
}

1;
