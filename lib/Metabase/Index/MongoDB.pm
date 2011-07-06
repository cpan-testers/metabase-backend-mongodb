use 5.010;
use strict;
use warnings;

package Metabase::Index::MongoDB;
# ABSTRACT: Metabase index on MongoDB

use Moose;
use Regexp::SQL::LIKE 0.001 qw/to_regexp/;
use re qw/regexp_pattern/;
use Try::Tiny;
use MongoDB;

with 'Metabase::Index';
with 'Metabase::Query';

# XXX eventually, do some validation on this -- dagolden, 2011-06-30
has 'host' => (
  is      => 'ro',
  isa     => 'Str',
  default => 'mongodb://localhost:27017',
  required  => 1,
);

# XXX eventually, do some validation on this -- dagolden, 2011-06-30
has 'db_name' => (
  is      => 'ro',
  isa     => 'Str',
  default => 'metabase',
  required  => 1,
);

# XXX eventually, do some validation on this -- dagolden, 2011-06-30
# e.g. if password,then also need non-empty username
has ['username', 'password'] => (
  is      => 'ro',
  isa     => 'Str',
  default => '',
);

has 'connection' => (
    is      => 'ro',
    isa     => 'MongoDB::Connection',
    lazy    => 1,
    default => sub {
        my $self = shift;
        return MongoDB::Connection->new(
          host => $self->host,
          $self->password ? (
            db_name   => $self->db_name,
            username  => $self->username,
            password  => $self->password,
          ) : (),
        );
    },
);

has 'collection_name' => (
  is      => 'ro',
  isa     => 'Str',
  default => 'metabase_index',
);

has 'coll' => (
    is      => 'ro',
    isa     => 'MongoDB::Collection',
    lazy    => 1,
    default => sub {
        my $self = shift;
        return $self->connection
                    ->get_database( $self->db_name )
                    ->get_collection( $self->collection_name );
    },
);

has 'sql_abstract' => (
    is      => 'ro',
    isa     => 'SQL::Abstract',
    lazy    => 1,
    default => sub {
        my $self = shift;
        return SQL::Abstract->new(
          case => 'lower',
          quote_char => q{`},
        );
    },
);

sub _munge_keys {
  my ($self, $data, $from, $to) = @_;
  $from ||= '.';
  $to ||= '|';

  for my $key (keys %$data) {
    (my $new_key = $key) =~ s/\Q$from\E/$to/;
    $data->{$new_key} = delete $data->{$key};
  }
  return $data;
}

sub add {
    my ( $self, $fact ) = @_;

    Carp::confess("can't index a Fact without a GUID") unless $fact->guid;

    my $metadata = $self->clone_metadata( $fact );
    $self->_munge_keys($metadata, '.' => '|');

    return $self->coll->insert( $metadata, {safe => 1} );
}

sub count {
    my ( $self, %spec) = @_;

    # XXX eventually, do something with %spec

    return $self->coll->count;
}

sub search {
    my ( $self, %spec) = @_;

    my ($sql, $limit) = $self->_get_search_sql("select ItemName()", %spec );

    # prepare request
    my $request = { SelectExpression => $sql };
    my $result = [];

    # gather results until all items received
    FETCH: {
      my $response;
      try {
        $response = $self->simpledb->send_request( 'Select', $request )
      } catch {
        Carp::confess("Got error '$_' from '$sql'");
      };

      if ( exists $response->{SelectResult}{Item} ) {
        my $items = $response->{SelectResult}{Item};
        # the following may not be necessary as of SimpleDB::Class 1.0000
        $items = [ $items ] unless ref $items eq 'ARRAY';
        push @$result, map { $_->{Name} } @$items ;

      }
      if ( exists $response->{SelectResult}{NextToken} ) {
        last if defined $limit && @$result >= $limit;
        $request->{NextToken} = $response->{SelectResult}{NextToken};
        redo FETCH;
      }
    }

    if ( defined $limit && @$result > $limit ) {
      splice @$result, $limit;
    }

    return $result;
}

sub exists {
    my ( $self, $guid ) = @_;
    return scalar @{ $self->search( 'core.guid' => lc $guid ) };
}

# DO NOT lc() GUID
sub delete {
    my ( $self, $guid ) = @_;

    Carp::confess("can't delete without a GUID") unless $guid;

    my $query = $self->_munge_keys( { 'core.guid' => $guid }, '.' => '|' );

    try { $self->coll->remove($query, { safe => 1 }) };

    # XXX should we be confessing on a failed delete? -- dagolden, 2011-06-30
    return $@ ? 0 : 1;
}

#--------------------------------------------------------------------------#
# Implement Metabase::Query requirements
#--------------------------------------------------------------------------#

sub translate_query {
  my ( $self, $spec ) = @_;

  my $query = {};

  # translate search query
  if ( defined $spec->{-where} and ref $spec->{-where} eq 'ARRAY') {
    $query = $self->dispatch_query_op( $spec->{-where} );
  }

  # translate query modifiers
  my $options = {};

  if ( defined $spec->{-order} and ref $spec->{-order} eq 'ARRAY') {
    my @order = @{$spec->{-order}};
    while ( @order ) {
      my ($dir, $field) = splice( @order, 0, 2);
      push @{$options->{sort_by}}, $field, $dir ? 1 : -1;
    }
  }

  if ( defined $spec->{-limit} ) {
    $options->{sort_by}{limit} = $spec->{-limit};
  }

  return $query, $options;
}

sub op_eq {
  my ($self, $field, $val) = @_;
  return $self->_munge_keys( { $field, $val } );
}

sub op_ne {
  my ($self, $field, $val) = @_;
  return $self->_munge_keys( { $field, { '$ne', $val } } );
}

sub op_gt {
  my ($self, $field, $val) = @_;
  return $self->_munge_keys( { $field, { '$gt', $val } } );
}

sub op_lt {
  my ($self, $field, $val) = @_;
  return $self->_munge_keys( { $field, { '$lt', $val } } );
}

sub op_ge {
  my ($self, $field, $val) = @_;
  return $self->_munge_keys( { $field, { '$gte', $val } } );
}

sub op_le {
  my ($self, $field, $val) = @_;
  return $self->_munge_keys( { $field, { '$lte', $val } } );
}

sub op_between {
  my ($self, $field, $low, $high) = @_;
  return $self->_munge_keys( { $field, { '$gte' => $low, '$lte' => $high } } );
}

sub op_like {
  my ($self, $field, $val) = @_;
  my ($re) = regexp_pattern(to_regexp($val));
  return $self->_munge_keys( { $field, { '$regex' => $re } } );
}

my %can_negate = map { $_ => 1 } qw(
  -ne -lt -le -gt -ge -between  
);

sub op_not {
  my ($self, $pred) = @_;
  my $op = $pred->[0];
  if ( ! $can_negate{$op} ) {
    Carp::confess( "Cannot negate '$op' operation\n" );
  }
  my $clause = $self->dispatch_query_op($pred);
  for my $k ( keys %$clause ) {
    $clause->{$k} = { '$not' => $clause->{$k} };
  }
  return $self->_munge_keys($clause);
}

sub op_or {
  my ($self, @args) = @_;
  state $depth = 0;
  if ( $depth++ ) {
    Carp::confess( "Cannot next '-or' predicates\n" );
  }
  my @predicates = map { $self->dispatch_query_op($_) } @args;
  $depth--;
  return { '$or' => \@predicates };
}

# AND has to flatten criteria into a single hash, but that means
# there are several things that don't work and we have to croak
sub op_and {
  my ($self, @args) = @_;

  my $query = {};
  while ( my $pred = shift @args ) {
    my $clause = $self->dispatch_query_op( $pred );
    for my $field ( keys %$clause ) {
      if ( exists $query->{$field} ) {
        if ( ref $query->{$field} ne 'HASH' ) {
          Carp::croak("Cannot '-and' equality with other operations");
        }
        _merge_hash( $field, $query, $clause );
      }
      else {
        $query->{$field} = $clause->{$field};
      }
    }
  }

  return $query;
}

sub _merge_hash {
  my ( $field, $orig, $new ) = @_;
  for my $op ( keys %{$new->{$field}} ) {
    if ( exists $orig->{$field}{$op} ) {
      Carp::confess( "Cannot merge '$op' criteria for '$field'\n" );
    }
    $orig->{$field}{$op} = $new->{$field}{$op};
  }
  return;
}

1;

__END__

=for Pod::Coverage::TrustPod add search exists delete count

=head1 SYNOPSIS

  require Metabase::Index::SimpleDB;
  Metabase::Index:SimpleDB->new(
    access_key_id => 'XXX',
    secret_access_key => 'XXX',
    domain     => 'metabase',
  );

=head1 DESCRIPTION

Metabase index using Amazon SimpleDB.

=head1 USAGE

See L<Metabase::Index> and L<Metabase::Librarian>.

=head1 BUGS

Please report any bugs or feature using the CPAN Request Tracker.
Bugs can be submitted through the web interface at
L<http://rt.cpan.org/Dist/Display.html?Queue=Metabase>

When submitting a bug or request, please include a test-file or a patch to an
existing test-file that illustrates the bug or desired feature.

=head1 COPYRIGHT AND LICENSE

Portions Copyright (c) 2010 by Leon Brocard

Licensed under terms of Perl itself (the "License").
You may not use this file except in compliance with the License.
A copy of the License was distributed with this file or you may obtain a
copy of the License from http://dev.perl.org/licenses/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

=cut
