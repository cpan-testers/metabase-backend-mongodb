use 5.006;
use strict;
use warnings;

package Metabase::Index::MongoDB;
# ABSTRACT: Metabase index on MongoDB

use Moose;
use SQL::Abstract 1;
use Try::Tiny;

with 'Metabase::Index';

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

sub add {
    my ( $self, $fact ) = @_;

    Carp::confess("can't index a Fact without a GUID") unless $fact->guid;

    my $metadata = $self->clone_metadata( $fact );

    my $i = 0;
    my @attributes;
    foreach my $key ( keys %$metadata ) {
        my $value = $metadata->{$key};
        push @attributes,
            "Attribute.$i.Name"    => $key,
            "Attribute.$i.Value"   => $value,
            # XXX not using replace is an optimization -- dagolden, 2010-04-29
#            "Attribute.$i.Replace" => 'true'; # XXX optimization -- dagolden, 2010-04-29
        $i++;
    }

    my $response = $self->simpledb->send_request(
        'PutAttributes',
        {   DomainName => $self->domain,
            ItemName   => lc $fact->guid,
            @attributes,
        }
    );
}

sub _get_search_sql {
  my ( $self, $select, %spec ) = @_;

  # extract limit and ordering keys
  my $limit = delete $spec{-limit};
  my  %order;
  for my $k ( qw/-asc -desc/ ) {
    $order{$k} = delete $spec{$k} if exists $spec{$k};
  }
  if (scalar keys %order > 1) {
    Carp::confess("Only one of '-asc' or '-desc' allowed");
  }
  if ( $limit && ! scalar keys %order ) {
    Carp::confess("-limit requires -asc or -desc");
  }

  # generate SimpleDB dialect of SQL
  my ($stmt, @bind) = $self->sql_abstract->where(\%spec, \%order);
  my ($where, @rest) = split qr/\?/, $stmt;
  for my $chunk (@rest) {
    # using double quotes, so double them first
    (my $val = shift @bind) =~ s{"}{""}g;
    $where .= qq{"$val"} . $chunk;
  }
  $where .= " limit $limit" if defined $limit && $limit > 0;
  my $domain = $self->domain;
  my $sql = qq{$select from `$domain` $where};
  return wantarray ? ($sql, $limit) : $sql;
}

sub count { 
    my ( $self, %spec) = @_;

    my ($sql, $limit) = $self->_get_search_sql("select count(*)", %spec );

    # prepare request
    my $request = { SelectExpression => $sql };
    my $result = 0;

    # gather results until all counts received
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
        for my $i (@$items) {
          next unless $i->{Name} eq 'Domain';
          $result += $i->{Attribute}{Value};
        }
      }
      if ( exists $response->{SelectResult}{NextToken} ) {
        last if defined $limit && @$result >= $limit;
        $request->{NextToken} = $response->{SelectResult}{NextToken};
        redo FETCH;
      }
    }

    return $result;
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

    my $response = $self->simpledb->send_request(
        'DeleteAttributes',
        {   DomainName => $self->domain,
            ItemName   => $guid,
        }
    );
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
