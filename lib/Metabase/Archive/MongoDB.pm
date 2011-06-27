use 5.006;
use strict;
use warnings;

package Metabase::Archive::MongoDB;
# ABSTRACT: Metabase storage using MongoDB

use Moose;
use Moose::Util::TypeConstraints;
use MooseX::Types::Path::Class;

use Metabase::Fact;
use Carp       ();

with 'Metabase::Archive';

has 'prefix' => (
    is       => 'ro',
    isa      => 'PrefixStr',
    required => 1,
    coerce   => 1,
);

# given fact, store it and return guid;
sub store {
    my ( $self, $fact_struct ) = @_;
    my $guid = $fact_struct->{metadata}{core}{guid};
    my $type = $fact_struct->{metadata}{core}{type};

    unless ($guid) {
        Carp::confess "Can't store: no GUID set for fact\n";
    }

    my $json = $self->_json->encode($fact_struct);

    if ( $self->compressed ) {
        $json = compress($json);
    }

    my $s3_object = $self->s3_bucket->object(
        key          => $self->prefix . lc $guid,
#        acl_short    => 'public-read',
        content_type => 'application/json',
    );
    $s3_object->put($json);

    return $guid;
}

# given guid, retrieve it and return it
# type is directory path
# class isa Metabase::Fact::Subclass
sub extract {
    my ( $self, $guid ) = @_;

    my $s3_object = $self->s3_bucket->object( key => $self->prefix . lc $guid );
    return $self->_extract_struct( $s3_object );
}

sub _extract_struct {
  my ( $self, $s3_object ) = @_;

  my $json = $s3_object->get;
  if ( $self->compressed ) {
    $json = uncompress($json);
  }
  my $struct  = $self->_json->decode($json);
  return $struct;
}

# DO NOT lc() GUID
sub delete {
    my ( $self, $guid ) = @_;

    my $s3_object = $self->s3_bucket->object( key => $self->prefix . $guid );
    $s3_object->delete;
}

sub iterator {
  my ($self) = @_;
  return Data::Stream::Bulk::Filter->new(
    stream => $self->s3_bucket->list( { prefix => $self->prefix } ),
    filter => sub {
      return [ map { $self->_extract_struct( $_ ) } @{ $_[0] } ];
    },
  );
}

1;

__END__

=for Pod::Coverage::TrustPod store extract delete iterator

=head1 SYNOPSIS

  require Metabase::Archive::MongoDB;
  Metabase::Archive::S3->new(
    access_key_id => 'XXX',
    secret_access_key => 'XXX',
    bucket     => 'acme',
    prefix     => 'metabase/',
    compressed => 0,
  );

=head1 DESCRIPTION

Store facts in Amazon S3.

=head1 USAGE

See L<Metabase::Archive> and L<Metabase::Librarian>.

TODO: document optional C<compressed> option (default 1) and
C<schema> option (sensible default provided).

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
