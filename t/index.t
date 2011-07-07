# Copyright (c) 2008 by Ricardo Signes. All rights reserved.
# Licensed under terms of Perl itself (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License was distributed with this file or you may obtain a 
# copy of the License from http://dev.perl.org/licenses/

use strict;
use warnings;

use Test::More 0.88;
use MongoDB;
use Try::Tiny;

use lib 't/lib';
use Test::Metabase::StringFact;

#-------------------------------------------------------------------------#
# Setup
#--------------------------------------------------------------------------#

my $conn = try{ MongoDB::Connection->new };
BAIL_OUT("No local mongod running for testing") unless $conn;

my $fact = Test::Metabase::StringFact->new(
  resource => 'cpan:///distfile/JOHNDOE/Foo-Bar-1.23.tar.gz',
  content  => "Hello World",
);

my $string = "Everything is fine"; # we need length later

my $fact2 = Test::Metabase::StringFact->new(
  resource => 'cpan:///distfile/JOHNDOE/Foo-Bar-1.23.tar.gz',
  content  => $string,
);

#--------------------------------------------------------------------------#
# Tests here
#--------------------------------------------------------------------------#

require_ok( 'Metabase::Index::MongoDB' );


my $testdb = 'test' . int(rand(2**21));

my $index = new_ok( 'Metabase::Index::MongoDB', [ db_name => $testdb ] );

is( $index->count, 0, "Index is empty" );

# add()
ok( $index->add( $fact ), "Indexed fact 1" );

# count()
is( $index->count, 1, "Index has one entry" );
is( $index->count(-where => [ -eq => 'core.type' => 'CPAN-Testers-Report' ]),
  0, "Count with (false) query condition is 0"
);
is( $index->count(-where => [ -eq => 'core.type' => 'Test-Metabase-StringFact']),
  1, "Count with (true) query condition is 1"
);

ok( $index->add( $fact2 ), "Indexed fact 2" );
is( $index->count, 2, "Index has two entries" );

is( $index->count(-where => [ -eq => 'core.guid' => $fact->guid]),
  1, "Count with (limited) query condition is 1"
);

# search()
my $matches;
$matches = $index->search( -where => [ -eq => 'core.guid' => $fact->guid ] );
is( scalar @$matches, 1, "Found one fact searching for guid" );

$matches = $index->search( -where => [ -eq => 'resource.cpan_id' => 'JOHNDOE'] );
is( scalar @$matches, 2, "Found two facts searching for resource cpan_id" );

$matches = $index->search( -where => [ -eq => 'core.type' => $fact->type ] ) ;
is( scalar @$matches, 2, "Found two facts searching for fact type" );

$matches = $index->search( -where => [ -eq => 'content.size' => length $string ] ) ;
is( scalar @$matches, 1, "Found one facts searching on content.size" );

is( $matches->[0], $fact2->guid, "Result GUID matches expected fact GUID" );

$matches = $index->search( -where => [ -eq => 'resource.author' => "asdljasljfa" ]);
is( scalar @$matches, 0, "Found no facts searching for bogus dist_author" );

$matches = $index->search( -where => [ -eq => bogus_key => "asdljasljfa"] );
is( scalar @$matches, 0, "Found no facts searching on bogus key" );


# delete()
ok( $index->delete( $fact->guid ), "Deleted fact 1 from index" );
is( $index->count, 1, "Index has one entry" );
ok( $index->delete( $fact2->guid ), "Deleted fact 2 from index" );
is( $index->count, 0, "Index is empty" );


#--------------------------------------------------------------------------#
# teardown
#--------------------------------------------------------------------------#
$conn->get_database($testdb)->drop;
done_testing;
