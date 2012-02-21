package Data::RuledCluster::Strategy;
use strict;
use warnings;
use Carp;
use Data::Util qw(is_array_ref);

sub resolve { Carp::croak('Not implement') }

sub keys_from_args {
    my ( $class, $args ) = @_;
    return is_array_ref( $args->{key} ) ? @{ $args->{key} } : ( $args->{key} );
}

1;

