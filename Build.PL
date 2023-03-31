use strict;
use warnings;
use Module::Build;


my $builder = Module::Build->new(
    add_to_cleanup      => [ 'Yote-RecordStore-*' ],
    build_requires => {
        'Test::More' => 0,
        'Module::Build' => 0.40,
    },
    dist_author      => 'eric wolf <coyo@madyote.com>',
    dist_abstract      => 'store indexed bytes',
    create_makefile_pl => 'traditional',
    license            => 'perl',
    module_name        => 'Yote::RecordStore',
    release_status     => 'stable',
    test_files         => [ 't/silo.t',
                            't/recordstore.t',
                        ],
    requires => {
        'perl'              => 'v5.16',
    },
    );

$builder->create_build_script();
