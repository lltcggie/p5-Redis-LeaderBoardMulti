package Redis::LeaderBoardMulti::Script;

use Digest::SHA qw(sha1_hex);

sub new {
    my ($class, %args) = @_;
    my $self = bless {}, $class;

    $self->{script} = $args{script};
    $self->{retry_count} = $args{retry_count} || 10;
    if ($args{use_evalsha}) {
        $self->{sha} = sha1_hex($self->{script});
    }

    return $self;
}

sub eval {
    my ($self, $redis, $keys, $args) = @_;
    my $ret;
    if (my $sha = $self->{sha}) {
        my $err;
        for (1..$self->{retry_count}) {
            $ret = eval { $redis->evalsha($sha, scalar(@$keys), @$keys, @$args) };
            $err = $@;
            if ($err && $err =~ /\[evalsha\] NOSCRIPT No matching script/i) {
                $self->load($redis);
                next;
            } elsif ($err) {
                die $err;
            }
            last;
        }
        die $err if $err;
    } else {
        $ret = $redis->eval($self->{script}, scalar(@$keys), @$keys, @$args);
    }

    return (wantarray && ref $ret eq 'ARRAY') ? @$ret : $ret;
}

sub exists {
    my ($self, $redis) = @_;
    if (my $sha = $self->{sha}) {
        return $redis->script_exists($sha)->[0];
    }
    return 1;
}

sub load {
    my ($self, $redis) = @_;
    if (my $sha = $self->{sha}) {
        my $redis_sha = $redis->script_load($self->{script});
        if (lc $sha ne lc $redis_sha) {
            die "SHA is unmatch (expected $sha but redis returns $redis_sha)";
        }
        return $sha;
    }
    return sha1_hex($self->{script});
}

1;
