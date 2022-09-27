#!/usr/bin/perl

use strict;
use warnings;
use CGI;
use DBI;

my $q = CGI->new;
print $q->header();

my $email = $q->param('email') || 'udbbwscdnbegrmloghuf@london.com';

my $config = {database => 'logparser', username => 'logparseruser', password => 'password4log'};

my $dbh = DBI->connect("dbi:Pg:dbname=$config->{database}", $config->{username}, $config->{password}, 
  {AutoCommit => 0, RaiseError => 1});

drawForm();
my $data = fetchData($email);
drawResults($data);

#my $count = 0;
#for my $row (@$data) {
#    print ${$row}[0]."---".${$row}[1]."\n";
#    $count++;
#}
#print "count = $count\n";
$dbh->disconnect;

sub drawForm {
  print '<form name="search" action="#"><input name="recipient" type="text"></form>';
}

sub makeTableRow {
  my $rowData = shift;
  return "<tr class='logparser-table__row'><td class='logparser-table__cell'>${$rowData}[0]</td><td>${$rowData}[1]</td></tr>"; 
}

sub makeLimitReached {
  return '<tr class="logparser-table__row_limit-reached"></tr>';  
}

sub drawResults {
  my $tableData = shift;  
  print "<table class='logparser-table'>";
  my $count = 0;
  for my $row (@$data) {
    print makeTableRow($row);
    $count++;
    if ($count == 101) {
      print makeLimitReached();
      break;
    }
  }
  print "</table>";  
}

sub fetchData {
  my $email = shift;
  my @result;
  my $select = 'select created, str from log where address = :adr order by created desc, int_id limit 101';
  my $sth = $dbh->prepare($select);
  $sth->bind_param(':adr', $email);
  $sth->execute;

  while (my @row = $sth->fetchrow_array) {
    push(@result, \@row);
  }

  return \@result;
}