package Data::RuledCluster::Strategy::Database;
use strict;
use warnings;
use parent 'Data::RuledCluster::Strategy';
use Carp ();

sub resolve {
    my ( $class, $resolver, $node_or_cluster, $args, $options ) = @_;

    my @keys = $class->keys_from_args($args);

    my $sql = $args->{sql}
        or Carp::croak('sql settings must be required');

    my $dbh = $options->{dbh}
        or Carp::croak('options has not dbh field');

    my ($resolved_node) = $dbh->selectrow_array($sql, undef, @keys);

    return ($resolved_node, @keys);
}

1;

