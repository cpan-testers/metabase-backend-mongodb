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
use Test::Metabase::Util;
my $TEST = Test::Metabase::Util->new;

#-------------------------------------------------------------------------#
# Setup
#--------------------------------------------------------------------------#

my $conn = try{ MongoDB::Connection->new };
BAIL_OUT("No local mongod running for testing") unless $conn;

#--------------------------------------------------------------------------#
# Tests here
#--------------------------------------------------------------------------#

require_ok( 'Metabase::Index::MongoDB' );


my $testdb = 'test' . int(rand(2**21));

my $index = new_ok( 'Metabase::Index::MongoDB', [ db_name => $testdb ] );

is( $index->count, 0, "Index is empty" );

ok( my $fact = $TEST->test_fact, "Created a fact" );
isa_ok( $fact, 'Test::Metabase::StringFact' );

ok( $index->add( $fact ), "Indexed fact" );
is( $index->count, 1, "Index has one entry" );

#my $matches;
#$matches = $index->search( 'core.guid' => $guid );
#is( scalar @$matches, 1, "found guid searching for guid" );
#
#$matches = $index->search( 'resource.cpan_id' => 'JOHNDOE' );
#ok( scalar @$matches >= 1, "found guid searching for resource cpan_id" );
#
#$matches = $index->search( 'core.type' => $fact->type );
#ok( scalar @$matches >= 1, "found guid searching for fact type" );
#
#$matches = $index->search( 'resource.author' => "asdljasljfa" );
#is( scalar @$matches, 0, "found no guids searching for bogus dist_author" );
#
#$matches = $index->search( bogus_key => "asdljasljfa" );
#is( scalar @$matches, 0, "found no guids searching on bogus key" );
#

#--------------------------------------------------------------------------#
# teardown
#--------------------------------------------------------------------------#
$conn->get_database($testdb)->drop;
done_testing;
