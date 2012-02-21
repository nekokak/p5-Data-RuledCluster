use strict;
use warnings;
use Test::More;
use Data::RuledCluster;

my $dr = Data::RuledCluster->new(
    config   => undef,
    callback => undef,
);

subtest 'basic method' => sub {
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

    ok $dr->is_cluster('USER_W');
    ok $dr->is_cluster('USER_R');
    ok $dr->is_node('USER001_W');
    ok $dr->is_node('USER002_W');
    ok $dr->is_node('USER001_R');
    ok $dr->is_node('USER002_R');

    my $nodes;
    $nodes = $dr->clusters('USER_W');
    note explain $nodes;
    is_deeply $nodes, [qw/USER001_W USER002_W/];

    $nodes = $dr->clusters('USER_R');
    note explain $nodes;
    is_deeply $nodes, [qw/USER001_R USER002_R/];
};

subtest 'default Key Strategy' => sub {
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

    my $node_info;
    $node_info = $dr->resolve('USER_W', 1);
    note explain $node_info;
    is_deeply $node_info, ['dbi:mysql:user002', 'root', '',];

    $node_info = $dr->resolve('USER_W', 2);
    note explain $node_info;
    is_deeply $node_info, ['dbi:mysql:user001', 'root', '',];

    my $resolve_node_keys = $dr->resolve_node_keys('USER_W', [qw/1 2 3/]);
    note explain $resolve_node_keys;
    is_deeply $resolve_node_keys, +{
        'USER001_W' => [
            '2'
        ],
        'USER002_W' => [
            '1',
            '3'
        ],
    };
};

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
