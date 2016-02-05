use strict;
use warnings;
use Test::More;
use Test::RedisServer;
use Redis::LeaderBoardMulti::Util qw/multi_exec watch_multi_exec/;
use Time::HiRes qw/time sleep/;
use Test::TCP qw/wait_port/;
use Net::EmptyPort qw/empty_port/;
use Sub::Retry;

my $redis_backend = $ENV{REDIS_BACKEND} || 'Redis';
eval "use $redis_backend";

eval { Test::RedisServer->new } or plan skip_all => 'redis-server is required in PATH to run this test';

# NOTE:
# Test::RedisServer uses UNIX domain sockets by default, but we use TCP/IP
# because the test of reconnection does not work with UNIX domain docket :(
my $redis_server = retry 3, 1, sub {
    my $port = empty_port();
    my $redis_server = Test::RedisServer->new(conf => {port => $port});
    wait_port($port);
    return $redis_server;
};

my $redis = $redis_backend->new( $redis_server->connect_info );
my $redis2 = $redis_backend->new( $redis_server->connect_info );

subtest 'multi_exec basic' => sub {
    $redis->flushall;

    my $start = time;
    my $pid = fork;
    BAIL_OUT("Cannot fork: $!") unless defined $pid;
    if ($pid == 0) {
        # child process
        while (time - $start < 3) {
            multi_exec $redis, 2, sub {
                my $r = shift;
                $r->incr('foo', sub {});
                $r->incr('bar', sub {});
            };
        }
        exit;
    }

    my $count;
    my $err_count = 0;
    while (time - $start < 3) {
        my ($foo, $bar) = multi_exec $redis, 2, sub {
            my $r = shift;
            $r->get('foo');
            $r->get('bar');
        };
        $count++;
        $err_count++ if ($foo || 0) != ($bar || 0);
    }
    note "incremented $count";
    is $err_count, 0, 'the value of `foo` always equals the value of `bar`';

    multi_exec $redis, 2, sub {
        my $r = shift;
        is $r->get('foo'), 'QUEUED', 'commands are QUEUED in the transaction';
    };

    waitpid $pid, 0;
};


subtest 'watch_multi_exec basic' => sub {
    $redis->flushall;

    $redis->set('foo', 0);

    my $is_retry = 0;
    watch_multi_exec $redis, ['foo'], 10, sub {
        my $r = shift;
        note 'GET foo';
        my $foo = $r->get('foo');
        if ($is_retry) {
            is $foo, 1, 'someone changed `foo`. my change is lost.';
        } else {
            is $foo, 0, 'nobody changes `foo`.';
        }
    }, sub {
        my ($r, $foo) = @_;

        if (!$is_retry) {
            is $redis2->set('foo', 1), 'OK', 'another client changes `foo`';
        }

        note "SET foo, $foo + 1";
        is $r->set('foo', $foo + 10), 'QUEUED', 'SET command is queued.';
        $is_retry = 1;
    };

    is $redis->get('foo'), 11, 'my change is executed.';
};

subtest 'watch_multi_exec reconnect' => sub {
    $redis->flushall;
    $redis->mset(foo => 1, bar => 1);

    my $start = time;
    my $pid = fork;
    BAIL_OUT("Cannot fork: $!") unless defined $pid;
    if ($pid == 0) {
        # child process
        # kill my test client
        while (time - $start < 5) {
            sleep 0.05;
            for my $client (split /\n/, $redis->client_list()) {
                my %info = (map { split /=/, $_, 2 } split / /, $client);
                if ($info{name} eq 'my-exec-client') {
                    $redis->client_kill($info{addr});
                }
            }
        }
        exit 0;
    }

    my $r = $redis_backend->new( $redis_server->connect_info, reconnect => 1, name => 'my-exec-client' );

    my $err_count = 0;
    my $retry_count = 0;
    while (time - $start < 5) {
        eval {
            my ($last_foo, $last_bar);
            watch_multi_exec $r, ['foo', 'bar'], 10, sub {
                my ($foo, $bar) = $r->mget('foo', 'bar');
                if ($last_foo && $last_bar) {
                    $retry_count++;
                    $err_count++ if $last_foo != $foo && $last_bar != $bar;
                }
                ($last_foo, $last_bar) = ($foo, $bar);
                return $foo, $bar;
            }, sub {
                my (undef, $foo, $bar) = @_;
                $r->set('foo', $foo + 1);
                $r->set('bar', $bar + 1);
            };
        };
        if (my $err = $@) {
            note $err;
        }
    }

    waitpid $pid, 0;
    note "recconection triggered $retry_count times";
    is $err_count, 0, 'no error occured';
    is $r->get('foo'), $r->get('bar'), 'exceuting INCR foo and INCR bar is atomic';
};

done_testing;
