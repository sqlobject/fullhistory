# A translation of the examples from:
#   http://www.perl.com/pub/a/2003/07/15/nocode.html

########################################
## Perl
#   package Bookworm::Setup;
#   use strict;
#   use DBI;
#
#   # Hash for table creation SQL - keys are the names of the tables,
#   # values are SQL statements to create the corresponding tables.
#   my %sql = (
#       author => qq {
#           CREATE TABLE author (
#               uid   int(10) unsigned NOT NULL auto_increment,
#               name  varchar(200),
#               PRIMARY KEY (uid)
#           )
#       },
#       book => qq{
#           CREATE TABLE book (
#               uid           int(10) unsigned NOT NULL auto_increment,
#               title         varchar(200),
#               first_name    varchar(200),
#               author        int(10) unsigned, # references author.uid
#               PRIMARY KEY (uid)
#           )
#       },
#       review => qq{
#           CREATE TABLE review (
#               uid       int(10) unsigned NOT NULL auto_increment,
#               book      int(10) unsigned, # references book.uid
#               reviewer  int(10) unsigned, # references reviewer.uid
#               PRIMARY KEY (uid)
#           )
#       },
#       reviewer => qq{
#           CREATE TABLE review (
#               uid   int(10) unsigned NOT NULL auto_increment,
#               name  varchar(200),
#               PRIMARY KEY (uid)
#           )
#       }
#   );


from SQLObject import *

class Author(SQLObject):
    _idName = 'uid'
    name = StringCol(length=200)

class Book(SQLObject):
    _idName = 'uid'
    title = StringCol(length=200)
    firstName = StringCol(length=200)
    author = ForeignKey('Author')
    reviews = MultipleJoin('Review')

class Review(SQLObject):
    _idName = 'uid'
    book = ForeignKey('Book')
    reviewer = ForeignKey('Reviewer')

class Reviewer(SQLObject):
    _idName = 'uid'
    name = StringCol(length=200)
    reviews = MultipleJoin('Review')

########################################
## Perl
#
#     setup_db( dbname      => 'bookworms',
#               dbuser      => 'username',
#               dbpass      => 'password',
#               force_clear => 0            # optional, defaults to 0
#             );
#
#   Sets up the tables. Unless "force_clear" is supplied and set to a
#   true value, any existing tables with the same names as we want to
#   create will be left alone, whether or not they have the right
#   columns etc. If "force_clear" is true, then any tables that are "in
#   the way" will be removed. _Note that this option will nuke all your
#   existing data._
#
#   The database user "dbuser" must be able to create and drop tables in
#   the database "dbname".
#
#   Croaks on error, returns true if all OK.

__connection__ == MySQLConnection(db='bookworms', user='username',
                                  password='password')
for cls in Author, Book, Review, Reviewer:
    cls._connection = __connection__

########################################
## Perl
#
#   package Bookworms::Template;
#   use strict;
#   use Bookworms::Config;
#   use CGI;
#   use Template;
#
#   # We have one method, which returns everything you need to send to
#   # STDOUT, including the Content-Type: header.
#
#   sub output {
#       my ($class, %args) = @_;
#
#       my $config = Bookworms::Config->new;
#       my $template_path = $config->get_var( "template_path" );
#       my $tt = Template->new( { INCLUDE_PATH => $template_path } );
#
#       my $tt_vars = $args{vars} || {};
#       $tt_vars->{site_name} = $config->get_var( "site_name" );
#
#       my $header = CGI::header;
#
#       my $output;
#       $tt->process( $args{template}, $tt_vars, \$output)
#           or croak $tt->error;
#       return $header . $output;

## @@: eh...

########################################
## Perl
#
#   package Bookworms::Book;
#   use base 'Bookworms::DBI';
#   use strict;
#
#   __PACKAGE__->set_up_table( "book" );
#   __PACKAGE__->has_a( author => "Bookworms::Author" );
#   __PACKAGE__->has_many( "reviews",
#                          "Bookworms::Review" => "book" );
#
#   1;

for cls in Book, Author, Review, Reviewer:
    cls.createTable(ifNotExists=True)

