use 5.010;
use strict;
use warnings;

use Test::More 0.92;
use Test::Deep;
use MongoDB;
use Try::Tiny;
use re 'regexp_pattern';

use lib 't/lib';
use Test::Metabase::StringFact;

#-------------------------------------------------------------------------#
# Setup
#--------------------------------------------------------------------------#

my $conn = try{ MongoDB::Connection->new };
BAIL_OUT("No local mongod running for testing") unless $conn;
my $testdb = 'test' . int(rand(2**21));
END { $conn->get_database($testdb)->drop; }

#--------------------------------------------------------------------------#
# Tests here
#--------------------------------------------------------------------------#

require_ok( 'Metabase::Index::MongoDB' );
my $index = new_ok( 'Metabase::Index::MongoDB', [ db_name => $testdb ] );

my @cases = (
  {
    label => 'single equality',
    input => { -where => [ -eq => 'content.grade' => 'PASS' ] },
    output => [ { 'content|grade' => 'PASS' }, {} ],
  },
  {
    label => "'-and' with equality",
    input => {
      -where => [
        -and =>
          [-eq => 'content.grade' => 'PASS' ],
          [-eq => 'content.osname' => 'MSWin32' ],
        ,
      ],
    },
    output => [
      {
        'content|grade' => 'PASS',
        'content|osname' => 'MSWin32',
      },
      {}
    ],
  },
  {
    label => 'inequality',
    input => { -where => [ -ne => 'content.grade' => 'PASS' ] },
    output => [ {'content|grade' => { '$ne' => 'PASS' }}, {} ],
  },
  {
    label => 'greater than',
    input => { -where => [ -gt => 'content.grade' => 'PASS' ] },
    output => [ {'content|grade' => { '$gt' => 'PASS' }}, {} ],
  },
  {
    label => 'less than',
    input => { -where => [ -lt => 'content.grade' => 'PASS' ] },
    output => [ {'content|grade' => { '$lt' => 'PASS' }}, {} ],
  },
  {
    label => 'greater than or equal to',
    input => { -where => [ -ge => 'content.grade' => 'PASS' ] },
    output => [ {'content|grade' => { '$gte' => 'PASS' }}, {} ],
  },
  {
    label => 'less than or equal to',
    input => { -where => [ -le => 'content.grade' => 'PASS' ] },
    output => [ {'content|grade' => { '$lte' => 'PASS' }}, {} ],
  },
  {
    label => 'between',
    input => { -where => [ -between => 'content.size' => 10 => 20 ] },
    output => [ {'content|size' => { '$gte' => 10, '$lte' => 20 }}, {} ],
  },
  {
    label => 'like',
    input => { -where => [ -like => 'core.resource' => '%JOHNDOE%'  ] },
    output => [
      {
        'core|resource' => { '$regex' => [regexp_pattern(qr/.*JOHNDOE.*/)]->[0] }
      },
      {}
    ],
  },
  {
    label => 'and',
    input => { -where => [ -and => [ -gt => 'content.size' => 5 ], [ -lt => 'content.size' => 10 ] ] },
    output => [
      { 'content|size' => { '$gt' => 5, '$lt' => 10 } },
      {}
    ],
  },
  {
    label => 'or',
    input => { -where => [ -or => [ -gt => 'content.size' => 15 ], [ -lt => 'content.size' => 5 ] ] },
    output => [
      { '$or' => [{ 'content|size' => { '$gt' => 15}}, { 'content|size' => { '$lt' => 5 } }] },
      {}
    ],
  },
  {
    label => 'not',
    input => { -where => [ -not => [ -gt => 'content.size' => 5 ] ] },
    output => [
      { 'content|size' => { '$not' => { '$gt' => 5 } } },
      {}
    ],
  },
  {
    label => 'ordering',
    input => {
      -where => [ -eq => 'content.grade' => 'PASS' ],
      -order => [ -desc => 'core.updated_time', -asc => 'core.resource' ],
    },
    output => [
      { 'content|grade' => 'PASS' },
      { sort_by => { 'core|updated_time' => -1, 'core|resource' => 1} },
    ],
  },
  {
    label => 'ordering plus limit',
    input => {
      -where => [ -eq => 'content.grade' => 'PASS' ],
      -order => [ -desc => 'core.updated_time', -asc => 'core.resource' ],
      -limit => 10,
    },
    output => [
      { 'content|grade' => 'PASS' },
      {
        sort_by => { 'core|updated_time' => -1, 'core|resource' => 1},
        limit => 10
      },
    ],
  },
);

for my $c ( @cases ) {
  my @query = $index->get_native_query( $c->{input} );
  cmp_deeply( \@query, $c->{output}, $c->{label} );
}

done_testing;
