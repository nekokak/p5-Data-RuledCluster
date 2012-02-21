use strict;
use warnings;
use Test::More;
use Data::RuledCluster;

my $dr = Data::RuledCluster->new(
    config   => undef,
    callback => undef,
);

subtest 'callback' => sub {
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
    $dr->config($config);
    $dr->{callback} = sub {
        my ($self, $node, $node_info) = @_;
        isa_ok $self, 'Data::RuledCluster';
        is $node, 'USER002_W';
        is_deeply $node_info, ['dbi:mysql:user002', 'root', '',];
    };

    $dr->resolve('USER_W', 1);

    my $callback = sub {
        my ($self, $node, $node_info) = @_;
        isa_ok $self, 'Data::RuledCluster';
        is $node, 'USER001_W';
        is_deeply $node_info, ['dbi:mysql:user001', 'root', '',];
    };

    $dr->resolve('USER_W', 2, +{callback => $callback});
};

done_testing;
