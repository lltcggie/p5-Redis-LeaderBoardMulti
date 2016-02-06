use strict;
use warnings;
use Test::More;
use Test::RedisServer;
use Redis::LeaderBoardMulti::Script;
use Time::HiRes qw/time sleep/;
use Test::TCP qw/wait_port/;
use Net::EmptyPort qw/empty_port/;
use Sub::Retry;

my $redis_backend = $ENV{REDIS_BACKEND} || 'Redis';
eval "use $redis_backend";

my $redis_server = eval { Test::RedisServer->new } or plan skip_all => 'redis-server is required in PATH to run this test';

my $redis = $redis_backend->new( $redis_server->connect_info );

subtest 'evalsha' => sub {
    my $s = Redis::LeaderBoardMulti::Script->new(
        script      => "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
        use_evalsha => 1,
    );

    ok !$s->exists($redis);
    is_deeply [$s->eval($redis, ['key1', 'key2'], ['arg1', 'arg2'])], ['key1', 'key2', 'arg1', 'arg2'];
    ok $s->exists($redis);
};

subtest 'eval' => sub {
    my $s = Redis::LeaderBoardMulti::Script->new(
        script => "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
    );

    ok $s->exists($redis);
    is_deeply [$s->eval($redis, ['key1', 'key2'], ['arg1', 'arg2'])], ['key1', 'key2', 'arg1', 'arg2'];
    ok $s->exists($redis);
};

done_testing;
