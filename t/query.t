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
  }
);

for my $c ( @cases ) {
  my @query = $index->get_native_query( $c->{input} );
  cmp_deeply( \@query, $c->{output}, $c->{label} );
}

done_testing;
