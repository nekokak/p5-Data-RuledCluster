use strict;
use warnings;
use Test::More;
use Data::RuledCluster;

my $dr = Data::RuledCluster->new(
    config   => undef,
);

subtest 'Strategy Complex' => sub {
    my $config = +{
        clusters => +{
            BIGDATA => +{
                nodes           => [qw/BIGDATA001 BIGDATA_CLUSTER/],
                strategy        => 'List',
                strategy_config => +{
                    '' => 'BIGDATA_CLUSTER',
                    BIGDATA001 => [qw/666/],
                },
            },
            BIGDATA_CLUSTER => [qw/BIGDATA002 BIGDATA003/],
        },
        node => +{
            BIGDATA001 => ['dbi:mysql:bigdata001', 'root', '',],
            BIGDATA002 => ['dbi:mysql:bigdata002', 'root', '',],
            BIGDATA003 => ['dbi:mysql:bigdata003', 'root', '',],
        },
    };
    $dr->config($config);

    my $node_info = $dr->resolve('BIGDATA', 1);
    note explain $node_info;
    is_deeply $node_info, +{node => 'BIGDATA003', node_info => ['dbi:mysql:bigdata003', 'root', '',]};
    is_deeply $dr->resolve('BIGDATA', 2),   +{node => 'BIGDATA002', node_info => ['dbi:mysql:bigdata002', 'root', '',]};
    is_deeply $dr->resolve('BIGDATA', 666), +{node => 'BIGDATA001', node_info => ['dbi:mysql:bigdata001', 'root', '',]};
};

done_testing;
