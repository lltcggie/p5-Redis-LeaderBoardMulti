use strict;
use warnings;
use Test::More;
use Test::RedisServer;
use Redis::LeaderBoardMulti::Script;

my $redis_backend = $ENV{REDIS_BACKEND} || 'Redis';
eval "use $redis_backend";

my $redis_server = eval { Test::RedisServer->new } or plan skip_all => 'redis-server is required in PATH to run this test';

my $redis = $redis_backend->new( $redis_server->connect_info );

subtest 'evalsha' => sub {
    $redis->script_flush;
    my $s = Redis::LeaderBoardMulti::Script->new(
        script      => "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
        use_evalsha => 1,
    );

    ok !$s->exists($redis), 'the script is not cached before EVAL';
    is_deeply [$s->eval($redis, ['key1', 'key2'], ['arg1', 'arg2'])], ['key1', 'key2', 'arg1', 'arg2'];
    ok $s->exists($redis), 'the script is cached after EVAL';
    is_deeply [$s->eval($redis, ['key1', 'key2'], ['arg1', 'arg2'])], ['key1', 'key2', 'arg1', 'arg2'];
};

subtest 'eval' => sub {
    $redis->script_flush;
    my $s = Redis::LeaderBoardMulti::Script->new(
        script      => "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
        use_evalsha => 0,
    );

    ok !$s->exists($redis), 'the script is not cached before EVAL';
    is_deeply [$s->eval($redis, ['key1', 'key2'], ['arg1', 'arg2'])], ['key1', 'key2', 'arg1', 'arg2'];
    ok $s->exists($redis), 'the script is cached after EVAL';
    is_deeply [$s->eval($redis, ['key1', 'key2'], ['arg1', 'arg2'])], ['key1', 'key2', 'arg1', 'arg2'];
};

subtest 'load' => sub {
    $redis->script_flush;
    my $s = Redis::LeaderBoardMulti::Script->new(
        script => "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
    );

    ok !$s->exists($redis), 'the script is not cached before EVAL';
    is lc $s->load($redis), lc $s->sha1, "loading script success";
    ok $s->exists($redis), 'the script is cached after EVAL';
};

done_testing;
