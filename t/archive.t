use 5.010;
use strict;
use warnings;

use Test::More;
use Test::Routine;
use Test::Routine::Util;
use Try::Tiny;

use MongoDB;
use Metabase::Archive::MongoDB;

has mongodb => (
  is => 'ro',
  isa => 'MongoDB::Connection',
  lazy_build => 1,
);

has dbname => (
  is => 'ro',
  isa => 'Str',
  default => sub { 'test' . int(rand(2**31)) },
);

sub _build_mongodb {
  my $conn = try{ MongoDB::Connection->new };
  BAIL_OUT("No local mongod running for testing") unless $conn;
  return $conn;
}

sub _build_archive {
  my $self = shift;
  return Metabase::Archive::MongoDB->new(
    db_name => $self->dbname
  );
}

after clear_archive => sub {
  my $self = shift;
  $self->mongodb->get_database( $self->dbname )->drop;
};

sub DEMOLISH { my $self = shift; $self->clear_archive }

run_tests(
  "Run Archive tests on Metabase::Index::MongodB",
  ["main", "Metabase::Test::Archive"]
);

done_testing;
