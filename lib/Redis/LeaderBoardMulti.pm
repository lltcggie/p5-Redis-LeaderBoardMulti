package Redis::LeaderBoardMulti;
use 5.008005;
use strict;
use warnings;

our $VERSION = "0.01";

use Redis::LeaderBoardMulti::Util qw/multi_exec watch_multi_exec/;

sub new {
    my ($class, %args) = @_;
    my $self = bless \%args, $class;

    return $self;
}

sub set_score {
    my ($self, $member, @scores) = @_;
    my $redis = $self->{redis};
    my $key = $self->{key};

    my $sub_sort_key = "$key:$member";
    my $packed_score = $self->_pack_scores(@scores);

    watch_multi_exec $redis, [$sub_sort_key], 10, sub {
        return $redis->get($sub_sort_key);
    }, sub {
        my (undef, $old_packed_score) = @_;
        $redis->zrem($key, "$old_packed_score$member", sub {}) if $old_packed_score;
        $redis->zadd($key, 0, "$packed_score$member", sub {});
        $redis->set($sub_sort_key, $packed_score, sub {});
    };
}

sub get_score {
    my ($self, $member) = @_;
    my $redis = $self->{redis};
    my $key = $self->{key};
    my $sub_sort_key = "$key:$member";
    my $packed_score = $redis->get($sub_sort_key);
    return $self->_unpack_scores($packed_score);
}

sub remove {
    my ($self, $member) = @_;
    my $redis = $self->{redis};
    my $key = $self->{key};
    my $sub_sort_key = "$key:$member";

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

sub get_sorted_order {
    my ($self, $member) = @_;
    my $redis = $self->{redis};
    my $key = $self->{key};
    my $sub_sort_key = "$key:$member";

    my ($order) = watch_multi_exec $redis, [$sub_sort_key], 10, sub {
        return $redis->get($sub_sort_key);
    }, sub {
        my (undef, $packed_score) = @_;
        $redis->zrank($key, "$packed_score$member", sub {});
    };
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

    my $packed_score;
    my ($rank) = watch_multi_exec $redis, [$sub_sort_key], 10, sub {
        $packed_score = $redis->get($sub_sort_key);
    }, sub {
        $redis->zlexcount($key, '-', "[$packed_score");
    };

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
    return pack('Q>' x scalar(@scores), @scores);
}

sub _unpack_scores {
    my ($self, $packed_score) = @_;
    return unpack('Q>' x 2, $packed_score);
}


1;
__END__

=encoding utf-8

=head1 NAME

Redis::LeaderBoardMulti - It's new $module

=head1 SYNOPSIS

    use Redis::LeaderBoardMulti;

=head1 DESCRIPTION

Redis::LeaderBoardMulti is ...

=head1 LICENSE

Copyright (C) Ichinose Shogo.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Ichinose Shogo E<lt>shogo82148@gmail.comE<gt>

=cut

