#!/usr/bin/perl

use strict;
use warnings;
use CGI;
use DBI;

my $ROWS_LIMIT = 100;
my $ROWS_LIMIT_MESSAGE = "Достигнут лимит на предельно допустимое количество строк в результате "
  ."поиска: $ROWS_LIMIT. Дальнейшие результаты поиска не отображены.";

my $q = CGI->new;
print $q->header(-charset    => 'utf-8');

my $email = $q->param('email') ;
#|| 'udbbwscdnbegrmloghuf@london.com';

my $config = {database => 'logparser', username => 'logparseruser', password => 'password4log'};

my $dbh = DBI->connect("dbi:Pg:dbname=$config->{database}", $config->{username}, $config->{password}, 
  {AutoCommit => 0, RaiseError => 1});

drawForm();
if ($email) {
  my $data = fetchData($email);
  drawResults($data);
}  

$dbh->disconnect;

sub drawForm {
  print '<form class="log-form" name="search" action="#">'
    .'<input class="log-form__input" name="email" type="text" placeholder="Поиск..."></form>';
}

sub makeTableRow {
  my $rowNum = shift;
  my $rowData = shift;
  return "<tr class='log-table__row'><td class='log-table__cell'>$rowNum</td>"
    ."<td class='log-table__cell'>${$rowData}[0]</td><td>${$rowData}[1]</td></tr>"; 
}

sub makeTableHeader {
  return "<tr><th class='log-table__header-cell'>№ п/п</th>"
    ."<th class='log-table__header-cell'>Дата и время запроса</th>"
    ."<th class='log-table__header-cell'>Содержание запроса</th></tr>";
}

sub makeLimitReached {
  return '<tr class="log-table__row log-table__row_limit-reached"><td colspan=3></td></tr>';  
}

sub drawResults {
  my $tableData = shift;  
  print "<table class='log-table'>";
  print makeTableHeader();
  my $count = 1;
  for my $row (@$tableData) {
    print makeTableRow($count, $row);
    $count++;
    if ($count == 101) {
      print makeLimitReached();
      last;
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