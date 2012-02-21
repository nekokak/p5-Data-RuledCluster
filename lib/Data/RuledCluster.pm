package Data::RuledCluster;
use 5.008_001;
use strict;
use warnings;
use Carp ();
use Class::Load ();
use Data::Util qw(is_array_ref is_hash_ref);

our $VERSION = '0.01';

sub new {
    my $class = shift;
    my %args = @_ == 1 ? %{$_[0]} : @_;
    bless \%args, $class;
}

sub resolver {
    my ($self, $strategy) = @_;

    my $pkg = $strategy;
    $pkg = $pkg =~ s/^\+// ? $pkg : "Data::RuledCluster::Strategy::$pkg";
    Class::Load::load_class($pkg);
    $pkg;
}

sub config {
    my ($self, $config) = @_;
    $self->{config} = $config;
}

sub resolve {
    my ($self, $cluster_or_node, $args, $opts) = @_;

    Carp::croak("missing mandatory config.") unless $self->{config};

    if ( $self->is_node($cluster_or_node) ) {
        my $node_info = $self->{config}->{node}->{$cluster_or_node};
        if ($opts->{get_node_name}) {
            return $cluster_or_node;
        }
        else {
            my $callback = $opts->{callback} || $self->{callback};
            return $callback ? $callback->($self, $cluster_or_node, $node_info) : $node_info;
        }
    }
    elsif ( $self->is_cluster($cluster_or_node) ) {
        if ( is_hash_ref($args) ) {
            Carp::croak("args has not 'strategy' field") unless $args->{strategy};
            my ( $resolved_node, @keys ) = $self->resolver($args->{strategy})->resolve(
                $self,
                $cluster_or_node,
                $args
            );
            return $self->resolve( $resolved_node, \@keys, $opts );
        }
        else {
            my $cluster_info = $self->cluster_info($cluster_or_node);
            if (is_array_ref($cluster_info)) {
                my ( $resolved_node, @keys ) = $self->resolver('Key')->resolve(
                    $self,
                    $cluster_or_node,
                    +{ key => $args, }
                );
                return $self->resolve( $resolved_node, \@keys, $opts );
            }
            elsif (is_hash_ref($cluster_info)) {
                my ( $resolved_node, @keys ) = $self->resolver($args->{strategy})->resolve(
                    $self,
                    $cluster_or_node,
                    +{ %$cluster_info, key => $args, }
                );
                return $self->resolve( $resolved_node, \@keys, $opts );
            }
        }
    }

    Carp::croak("$cluster_or_node is not defined.");
}

sub resolve_node_keys {
    my ($self, $cluster_or_node, $keys, $args) = @_;

    my %node_keys;
    for my $key ( @$keys ) {
        if ( is_hash_ref $args ) {
            $args->{strategy} ||= 'Key';
            $args->{key}        = $key;
        }
        else {
            $args = $key;
        }
        
        my $resolved_node = $self->resolve( $cluster_or_node, $args, +{get_node_name => 1} );
        $node_keys{$resolved_node} ||= [];
        push @{$node_keys{$resolved_node}}, $key;
    }

    return wantarray ? %node_keys : \%node_keys;
}

sub cluster_info {
    my ($self, $cluster) = @_;
    $self->{config}->{clusters}->{$cluster};
}

sub clusters {
    my ($self, $cluster) = @_;
    my $cluster_info = $self->cluster_info($cluster);
    my @nodes = is_array_ref($cluster_info) ? @$cluster_info : @{ $cluster_info->{nodes} };
    wantarray ? @nodes : \@nodes;
}

sub is_cluster {
    my ($self, $cluster) = @_;
    exists $self->{config}->{clusters}->{$cluster} ? 1 : 0;
}

sub is_node {
    my ($self, $node) = @_;
    exists $self->{config}->{node}->{$node} ? 1 : 0;
}

1;
__END__

=head1 NAME

Data::RuledCluster - Perl extention to do something

=head1 VERSION

This document describes Data::RuledCluster version 0.01.

=head1 SYNOPSIS

sample1:

    use Data::RuledCluster;
    use DBI;

    my $dr = Data::RuledCluster->new(
        config      => $config,
        callback    => sub {
            my $config = shift;
            DBI->connect($config);
        },
    );
    # or
    use YAML;
    my $dr = Data::RuledCluster->new(
        config      => YAML::LoadFile('/path/to/config.yaml'),
        callback    => sub {
            my $config = shift;
            DBI->connect($config);
        },
    );
    my $object = $dr->resolve('USER_W', $user_id);
    # or
    my $object = $dr->resolve('USER001_W');

    __END__
    # key cluster config
    +{
        clusters => +{
            USER_W => [qw/USER001_W USER002_W/],
            USER_R => [qw/USER001_R USER002_R/],
        },
        node => +{
            USER001_W => ['dbi:mysql:user001', 'root', '',],
            USER002_W => ['dbi:mysql:user002', 'root', '',],
            USER001_R => ['dbi:mysql:user001', 'root', '',],
            USER002_R => ['dbi:mysql:user002', 'root', '',],
        },
    };

sample2:

    use Data::RuledCluster;
    use RedisDB;

    my $dr = Data::RuledCluster->new(
        config      => $config,
        callback    => sub {
            my $config = shift;
            RedisDB->new($config);
        },
    );
    my $object = $dr->resolve('LB_W', $lb_id);
    # or
    my $object = $dr->resolve('LB001_W');

    __END__
    # key cluster config
    +{
        clusters => +{
            LB_W => [qw/LB001 LB002/],
            LB_B => [qw/LB001 LB002/],
        },
        node => +{
            LB001_W => [+{host => redis001, port => 6379,}],
            LB002_W => [+{host => redis002, port => 6379,}],
            LB001_B => [+{host => redis003, port => 6379,}],
            LB002_B => [+{host => redis004, port => 6379,}],
        },
    };

=head1 DESCRIPTION

# TODO

=head1 INTERFACE

=head2 Functions

=head3 C<< hello() >>

# TODO

=head1 DEPENDENCIES

Perl 5.8.1 or later.

=head1 BUGS

All complex software has bugs lurking in it, and this module is no
exception. If you find a bug please either email me, or add the bug
to cpan-RT.

=head1 SEE ALSO

L<perl>

=head1 AUTHOR

Atsushi Kobayashi E<lt>nekokak@gmail.comE<gt>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2012, Atsushi Kobayashi. All rights reserved.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
