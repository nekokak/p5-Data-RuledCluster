package Data::RuledCluster;
use 5.008_001;
use strict;
use warnings;
use Carp ();
use Class::Load ();
use Data::Util qw(is_array_ref is_hash_ref);

our $VERSION = '0.02';

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
    $self->{config} = $config if $config;
    $self->{config};
}

sub resolve {
    my ($self, $cluster_or_node, $args) = @_;

    Carp::croak("missing mandatory config.") unless $self->{config};

    if ( $self->is_cluster($cluster_or_node) ) {
        if ( is_hash_ref($args) ) {
            Carp::croak("args has not 'strategy' field") unless $args->{strategy};
            my ( $resolved_node, @keys ) = $self->resolver($args->{strategy})->resolve(
                $self,
                $cluster_or_node,
                $args
            );
            return $self->resolve( $resolved_node, \@keys );
        }
        else {
            my $cluster_info = $self->cluster_info($cluster_or_node);
            if (is_array_ref($cluster_info)) {
                my ( $resolved_node, @keys ) = $self->resolver('Key')->resolve(
                    $self,
                    $cluster_or_node,
                    +{ key => $args, }
                );
                return $self->resolve( $resolved_node, \@keys );
            }
            elsif (is_hash_ref($cluster_info)) {
                my ( $resolved_node, @keys ) = $self->resolver($cluster_info->{strategy})->resolve(
                    $self,
                    $cluster_or_node,
                    +{ %$cluster_info, key => $args, }
                );
                return $self->resolve( $resolved_node, \@keys );
            }
        }
    }
    elsif ( $self->is_node($cluster_or_node) ) {
        return +{
            node => $cluster_or_node,
            node_info => $self->{config}->{node}->{$cluster_or_node},
        };
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
        
        my $resolved = $self->resolve( $cluster_or_node, $args, +{get_node_name => 1} );
        $node_keys{$resolved->{node}} ||= [];
        push @{$node_keys{$resolved->{node}}}, $key;
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

Data::RuledCluster - clustering data resolver

=head1 VERSION

This document describes Data::RuledCluster version 0.01.

=head1 SYNOPSIS

    use Data::RuledCluster;
    
    my $config = +{
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
    my $dr = Data::RuledCluster->new(
        config => $config,
    );
    my $resolved_data = $dr->resolve('USER_W', $user_id);
    # or
    my $resolved_data = $dr->resolve('USER001_W');
    # $resolved_data: +{ node => 'USER001_W', node_info => ['dbi:mysql:user001', 'root', '',]}

=head1 DESCRIPTION

# TODO

=head1 METHOD

=item my $dr = Data::RuledCluster->new($config)

create a new Data::RuledCluster instance.

=item $dr->config($config)

set or get config.

=item $dr->resolve($cluster_or_node, $args)

resolve cluster data.

=item $dr->resolve_node_keys($cluster, $keys, $args)

Return hash resolved node and keys.

=item $dr->is_cluster($cluster_or_node)

If $cluster_or_node is cluster, return true.
But $cluster_or_node is not cluster, return false.

=item $dr->is_node($cluster_or_node)

If $cluster_or_node is node, return true.
But $cluster_or_node is not node, return false.

=item $dr->cluster_info($cluster)

Return cluster info hash ref.

=item $dr->clusters($cluster)

Retrieve cluster member node names as Array.

=head1 DEPENDENCIES

L<Class::Load>

L<Data::Util>

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
