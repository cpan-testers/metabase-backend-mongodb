use 5.010;
use strict;
use warnings;
use Test::More 0.92;
use Test::Deep;
use MongoDB::Connection;

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

);

for my $c ( @cases ) {
  my @query = $index->get_native_query( $c->{input} );
  cmp_deeply( \@query, $c->{output}, $c->{label} );
}

done_testing;
