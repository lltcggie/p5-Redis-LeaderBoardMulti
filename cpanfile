requires 'perl', '5.008001';
requires 'Redis::Script';
requires 'Redis::Transaction';

on 'test' => sub {
    requires 'Test::More', '0.98';
    requires 'Redis';
    requires 'Test::RedisServer';
};

