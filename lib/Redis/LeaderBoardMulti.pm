package Redis::LeaderBoardMulti;
use 5.008005;
use strict;
use warnings;

our $VERSION = "0.01";

use Redis::LeaderBoardMulti::Util qw/multi_exec watch_multi_exec/;
use Redis::LeaderBoardMulti::Script;

our $SUPPORT_64BIT = eval { unpack('Q>', "\x00\x00\x00\x00\x00\x00\x00\x01") };

sub new {
    my ($class, %args) = @_;
    my $self = bless {
        use_hash    => 1,
        use_script  => 1,
        use_evalsha => 1,
        order       => ['desc'],
        %args,
    }, $class;

    $self->{hash_key} ||= $self->{key} . ":score";

    return $self;
}

sub set_score {
    my ($self, $member, @scores) = @_;
    my $redis = $self->{redis};
    my $key = $self->{key};
    my $packed_score = $self->_pack_scores(@scores);

    if ($self->{use_hash}) {
        my $hash_key = $self->{hash_key};
        if ($self->{use_script}) {
            my $script = $self->{_set_score_hash_script} ||= Redis::LeaderBoardMulti::Script->new(
                use_evalsha => $self->{use_evalsha},
                script      => <<EOS,
local s=redis.call('HGET',KEYS[2],ARGV[1])
if s then
redis.call('ZREM',KEYS[1],s..ARGV[1])
end
redis.call('ZADD',KEYS[1],0,ARGV[2]..ARGV[1])
redis.call('HSET',KEYS[2],ARGV[1],ARGV[2])
EOS
            );
            $script->eval($redis, [$key, $hash_key], [$member, $packed_score]);
        } else {
            watch_multi_exec $redis, [$hash_key], 10, sub {
                return $redis->hget($hash_key, $member);
            }, sub {
                my (undef, $old_packed_score) = @_;
                $redis->zrem($key, "$old_packed_score$member", sub {}) if $old_packed_score;
                $redis->zadd($key, 0, "$packed_score$member", sub {});
                $redis->hset($hash_key, $member, $packed_score, sub {});
            };
        }
    } else {
        my $sub_sort_key = "$key:$member";
        if ($self->{use_script}) {
            my $script = $self->{_set_score_script} ||= Redis::LeaderBoardMulti::Script->new(
                use_evalsha => $self->{use_evalsha},
                script      => <<EOS,
local s=redis.call('GET',KEYS[2])
if s then
redis.call('ZREM',KEYS[1],s..ARGV[1])
end
redis.call('ZADD',KEYS[1],0,ARGV[2]..ARGV[1])
redis.call('SET',KEYS[2],ARGV[2])
EOS
            );
            $script->eval($redis, [$key, $sub_sort_key], [$member, $packed_score]);
        } else {
            watch_multi_exec $redis, [$sub_sort_key], 10, sub {
                return $redis->get($sub_sort_key);
            }, sub {
                my (undef, $old_packed_score) = @_;
                $redis->zrem($key, "$old_packed_score$member", sub {}) if $old_packed_score;
                $redis->zadd($key, 0, "$packed_score$member", sub {});
                $redis->set($sub_sort_key, $packed_score, sub {});
            };
        }
    }
}

sub get_score {
    my ($self, $member) = @_;
    my $redis = $self->{redis};
    my $key = $self->{key};
    my $packed_score = $self->{use_hash}
        ? $redis->hget($self->{hash_key}, $member)
        : $redis->get("$key:$member");
    return $self->_unpack_scores($packed_score);
}

sub remove {
    my ($self, $member) = @_;
    my $redis = $self->{redis};
    my $key = $self->{key};

    if ($self->{use_hash}) {
        my $hash_key = $self->{hash_key};
        if ($self->{use_script}) {
            my $script = $self->{_remove_hash_script} ||= Redis::LeaderBoardMulti::Script->new(
                use_evalsha => $self->{use_evalsha},
                script      => <<EOS,
local s=redis.call('HGET',KEYS[2],ARGV[1])
if s then
redis.call('ZREM',KEYS[1],s..ARGV[1])
redis.call('HDEL',KEYS[2],ARGV[1])
end
EOS
            );
            $script->eval($redis, [$key, $hash_key], [$member]);
        } else {
            watch_multi_exec $redis, [$hash_key], 10, sub {
                return $redis->hget($hash_key, $member);
            }, sub {
                my (undef, $packed_score) = @_;
                if ($packed_score) {
                    $redis->zrem($key, "$packed_score$member");
                    $redis->hdel($hash_key, $member);
                }
            };
        }
    } else {
        my $sub_sort_key = "$key:$member";
        if ($self->{use_script}) {
            my $script = $self->{_remove_script} ||= Redis::LeaderBoardMulti::Script->new(
                use_evalsha => $self->{use_evalsha},
                script      => <<EOS,
local s=redis.call('GET',KEYS[2])
if s then
redis.call('ZREM',KEYS[1],s..ARGV[1])
redis.call('DEL',KEYS[2])
end
EOS
            );
            $script->eval($redis, [$key, $sub_sort_key], [$member]);
        } else {
            watch_multi_exec $redis, [$sub_sort_key], 10, sub {
                return $redis->get($sub_sort_key);
            }, sub {
                my (undef, $packed_score) = @_;
                if ($packed_score) {
                    $redis->zrem($key, "$packed_score$member");
                    $redis->del($sub_sort_key);
                }
            };
        }
    }
}

sub get_sorted_order {
    my ($self, $member) = @_;
    my $redis = $self->{redis};
    my $key = $self->{key};
    my $order;

    if ($self->{use_hash}) {
        my $hash_key = $self->{hash_key};
        if ($self->{use_script}) {
            my $script = $self->{_get_sort_order_hash_script} ||= Redis::LeaderBoardMulti::Script->new(
                use_evalsha => $self->{use_evalsha},
                script      => <<EOS,
local s=redis.call('HGET',KEYS[2],ARGV[1])
return redis.call('ZRANK',KEYS[1],s..ARGV[1])
EOS
            );
            $order = $script->eval($redis, [$key, $hash_key], [$member]);
        } else {
            my $packed_score = $redis->hget($hash_key, $member);
            $order = $redis->zrank($key, "$packed_score$member");
        }
    } else {
        my $sub_sort_key = "$key:$member";
        if ($self->{use_script}) {
            my $script = $self->{_get_sort_order_script} ||= Redis::LeaderBoardMulti::Script->new(
                use_evalsha => $self->{use_evalsha},
                script      => <<EOS,
local s=redis.call('GET',KEYS[2])
return redis.call('ZRANK',KEYS[1],s..ARGV[1])
EOS
            );
            $order = $script->eval($redis, [$key, $sub_sort_key], [$member]);
        } else {
            ($order) = watch_multi_exec $redis, [$sub_sort_key], 10, sub {
                return $redis->get($sub_sort_key);
            }, sub {
                my (undef, $packed_score) = @_;
                $redis->zrank($key, "$packed_score$member", sub {});
            };
        }
    }
    return $order;
}

sub get_rank {
    my ($self, $member) = @_;
    my ($rank) = $self->get_rank_with_score($member);
    return $rank;
}

sub get_rank_with_score {
    my ($self, $member) = @_;
    my $redis = $self->{redis};
    my $key = $self->{key};
    my $sub_sort_key = "$key:$member";

    my $rank;
    my $packed_score;
    if ($self->{use_hash}) {
        my $hash_key = $self->{hash_key};
        if ($self->{use_script}) {
            my $script = $self->{_get_rank_with_score_hash_script} ||= Redis::LeaderBoardMulti::Script->new(
                use_evalsha => $self->{use_evalsha},
                script      => <<EOS,
local s=redis.call('HGET',KEYS[2],ARGV[1])
return {s,redis.call('ZLEXCOUNT',KEYS[1],'-','['..s)}
EOS
            );
            ($packed_score, $rank) = $script->eval($redis, [$key, $hash_key], [$member]);
        } else {
            $packed_score = $redis->hget($hash_key, $member);
            $rank = $redis->zlexcount($key, '-', "[$packed_score");
        }
    } else {
        my $sub_sort_key = "$key:$member";
        if ($self->{use_script}) {
            my $script = $self->{_get_rank_with_score_script} ||= Redis::LeaderBoardMulti::Script->new(
                use_evalsha => $self->{use_evalsha},
                script      => <<EOS,
local s=redis.call('GET',KEYS[2])
return {s,redis.call('ZLEXCOUNT',KEYS[1],'-','['..s)}
EOS
            );
            ($packed_score, $rank) = $script->eval($redis, [$key, $sub_sort_key], []);
        } else {
            ($rank) = watch_multi_exec $redis, [$sub_sort_key], 10, sub {
                $packed_score = $redis->get($sub_sort_key);
            }, sub {
                $redis->zlexcount($key, '-', "[$packed_score");
            };
        }
    }


    return $rank + 1, $self->_unpack_scores($packed_score);
}

sub get_rank_by_score {
    my ($self, @scores) = @_;
    my $redis = $self->{redis};
    my $key = $self->{key};

    my $packed_score = $self->_pack_scores(@scores);
    my $rank = $redis->zlexcount($key, '-', "[$packed_score");

    return $rank + 1;
}

sub _pack_scores {
    my ($self, @scores) = @_;
    my $num = scalar @scores;
    my $order = $self->{order};
    die "the number of scores is illegal" if $num != scalar @$order;
    my $packed_score = '';
    for my $i (0..$num-1) {
        my $score = $scores[$i];
        my $packed = $SUPPORT_64BIT ? pack('q>', $score) : pack('l>l>', $score < 0 ? -1 : 0, $score);
        $packed = "$packed" ^ "\x80";
        if ($order->[$i] eq 'asc') {
            $packed = ~"$packed";
        }
        $packed_score .= $packed;
    }
    return $packed_score;
}

sub _unpack_scores {
    my ($self, $packed_score) = @_;
    my $order = $self->{order};
    my $num = scalar @$order;
    my @packeds = $packed_score =~ /.{8}/g;
    my @scores;
    for my $i (0..$num-1) {
        my $packed = $packeds[$i];
        $packed = "$packed" ^ "\x80";
        if ($order->[$i] eq 'asc') {
            $packed = ~"$packed";
        }
        my $score;
        if ($SUPPORT_64BIT) {
            ($score) = unpack('q>', $packed);
        } else {
            (undef, $score) = unpack('l>l>', $packed)
        }
        push @scores, $score;
    }
    return @scores;
}


1;
__END__

=encoding utf-8

=head1 NAME

Redis::LeaderBoardMulti - Redis leader board considering multiple scores

=head1 SYNOPSIS

    use Redis;
    use Redis::LeaderBoard;
    my $redis = Redis->new;
    my $lb = Redis::LeaderBoardMulti->new(
        redis => $redis,
        key   => 'leader_board:1',
        order => ['asc', 'desc'], # asc/desc, desc as default
    );
    $lb->set_score('one' => 100, time);
    $lb->set_score('two' =>  50, time);
    my ($rank, $score) = $lb->get_rank_with_score('one');

=head1 DESCRIPTION

Redis::LeaderBoardMulti is for providing leader board by using Redis's sorted set.
Redis::LeaderBoard considers only one score, while Redis::LeaderBoardMulti can consider secondary score.

=head1 LICENSE

Copyright (C) Ichinose Shogo.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Ichinose Shogo E<lt>shogo82148@gmail.comE<gt>

=cut

