use strict;
use warnings;
use Test::More;
use Redis::LeaderBoardMulti;

sub pack_scores {
    my ($order, $scores) = @_;
    my $l = Redis::LeaderBoardMulti->new(
        key   => 'sortable-member',
        order => $order,
    );
    return $l->_pack_scores($scores);
}

sub unpack_scores {
    my ($order, $packed_scores) = @_;
    my $l = Redis::LeaderBoardMulti->new(
        key   => 'sortable-member',
        order => $order,
    );
    return $l->_unpack_scores($packed_scores);
}

sub test_unpack {
    my ($order, $scores) = @_;
    is_deeply [unpack_scores($order, pack_scores($order, $scores))], $scores;
}

sub test_32bit_integer {
    my ($order, $scores) = @_;
    my $packed = pack_scores($order, $scores);
    my $packed32 = do { local $Redis::LeaderBoardMulti::SUPPORT_64BIT = 0; pack_scores($order, $scores) };
    is $packed, $packed32;
}

subtest 'compare score' => sub {
    cmp_ok pack_scores(['desc'], [ 0]), 'lt', pack_scores(['desc'], [  1]);
    cmp_ok pack_scores([ 'asc'], [ 0]), 'gt', pack_scores([ 'asc'], [  1]);
    cmp_ok pack_scores(['desc'], [-1]), 'lt', pack_scores(['desc'], [  0]);
    cmp_ok pack_scores([ 'asc'], [-1]), 'gt', pack_scores([ 'asc'], [  0]);
    cmp_ok pack_scores(['desc'], [-2]), 'lt', pack_scores(['desc'], [ -1]);
    cmp_ok pack_scores([ 'asc'], [-2]), 'gt', pack_scores([ 'asc'], [ -1]);
    cmp_ok pack_scores(['desc'], [ 1]), 'lt', pack_scores(['desc'], [256]);
    cmp_ok pack_scores([ 'asc'], [ 1]), 'gt', pack_scores([ 'asc'], [256]);

    my $INT_MAX = ~0 >> 1;
    my $INT_MIN = -$INT_MAX - 1;
    cmp_ok pack_scores(['desc'], [$INT_MIN  ]), 'lt', pack_scores(['desc'], [$INT_MIN+1]);
    cmp_ok pack_scores([ 'asc'], [$INT_MIN  ]), 'gt', pack_scores([ 'asc'], [$INT_MIN+1]);
    cmp_ok pack_scores(['desc'], [$INT_MAX-1]), 'lt', pack_scores(['desc'], [$INT_MAX  ]);
    cmp_ok pack_scores([ 'asc'], [$INT_MAX-1]), 'gt', pack_scores([ 'asc'], [$INT_MAX  ]);
    cmp_ok pack_scores(['desc'], [$INT_MIN  ]), 'lt', pack_scores(['desc'], [$INT_MAX  ]);
    cmp_ok pack_scores([ 'asc'], [$INT_MIN  ]), 'gt', pack_scores([ 'asc'], [$INT_MAX  ]);
};

subtest 'compare multi scores' => sub {
    cmp_ok pack_scores(['desc', 'desc'], [0, 0]), 'lt', pack_scores(['desc', 'desc'], [0, 1]);
    cmp_ok pack_scores([ 'asc',  'asc'], [0, 0]), 'gt', pack_scores([ 'asc',  'asc'], [0, 1]);
};

subtest 'unpack' => sub {
    test_unpack(['desc'], [  0]);
    test_unpack(['desc'], [  1]);
    test_unpack(['desc'], [ -1]);
    test_unpack(['desc'], [256]);
    test_unpack(['desc'], [ 0x7FFFFFFF]);
    test_unpack(['desc'], [-0x80000000]);

    test_unpack(['asc'], [  0]);
    test_unpack(['asc'], [  1]);
    test_unpack(['asc'], [ -1]);
    test_unpack(['asc'], [256]);
    test_unpack(['asc'], [ 0x7FFFFFFF]);
    test_unpack(['asc'], [-0x80000000]);
};

subtest '64bit' => sub {
    plan skip_all => 'Your perl does not support 64bit integer' unless $Redis::LeaderBoardMulti::SUPPORT_64BIT;
    test_32bit_integer(['desc'], [ 0]);
    test_32bit_integer(['desc'], [ 1]);
    test_32bit_integer(['desc'], [-1]);
    test_32bit_integer(['desc'], [ 0x7FFFFFFF]);
    test_32bit_integer(['desc'], [-0x80000000]);
};

done_testing;
