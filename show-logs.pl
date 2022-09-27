#!/usr/bin/perl

use strict;
use warnings;
use CGI;
use DBI;

my $ROWS_LIMIT = 100;
my $ROWS_LIMIT_MESSAGE = "Достигнут лимит на предельно допустимое количество строк в результате "
  ."поиска. Дальнейшие результаты поиска не отображены.";
my $NO_RESULTS_MESSAGE = "Ничего не найдено";
my $BAD_EMAIL_MESSAGE = "Введенная строка не является электронным адресом";
my $q = CGI->new;

my $email = $q->param('email'); 
my $config = {database => 'logparser', username => 'logparseruser', password => 'password4log'};

my $dbh = DBI->connect("dbi:Pg:dbname=$config->{database}", $config->{username}, $config->{password}, 
  {AutoCommit => 0, RaiseError => 1});

print $q->header(-charset    => 'utf-8');

if (!$dbh) {
    print "<p>Не удалось подключиться к базе данных!</p>";
    exit;
}

print makeForm();
if ($email) {
  if (validateEmail($email)) {
    my $data = fetchData($email);
    print makeTableStart();
    print makeResults($data);
    print makeTableEnd();
  } else {
    print makeBadEmailEntered();
  }
}  

$dbh->disconnect;

sub validateEmail {
  my $string = shift;
  return $string && ($string =~ /[^@\s]+@[^@\s]+\.[^@\s]+/);
}

sub makeForm {
  return '<form class="log-form" name="search" action="#">'
    .'<input class="log-form__input" name="email" type="text" placeholder="Поиск..." size="30"></form>';
}

sub makeTableRow {
  my $rowNum = shift;
  my $rowData = shift;
  return "<tr class='log-table__row'><td class='log-table__cell'>$rowNum</td>"
    ."<td class='log-table__cell'>${$rowData}[0]</td><td>${$rowData}[1]</td></tr>"; 
}

sub makeTableStart {
  return "<table class='log-table' border='1' style='width: 100%;'>"
    ."<tr><th class='log-table__header-cell'>№ п/п</th>"
    ."<th class='log-table__header-cell'>Дата и время запроса</th>"
    ."<th class='log-table__header-cell'>Содержание запроса</th></tr>";
}

sub makeTableEnd {
  return "</table>";
}

sub makeLimitReached {
  return "<tr class='log-table__row log-table__row_type_limit-reached'><td colspan=3><center>$ROWS_LIMIT_MESSAGE</center></td></tr>";  
}

sub makeNoResultFound {
  return "<tr class='log-table__row log-table__row_type_no-results'><td colspan=3><center>$NO_RESULTS_MESSAGE</center></td></tr>";  
}

sub makeBadEmailEntered {
  return "<p>$BAD_EMAIL_MESSAGE</p>";  
}

sub makeResults {
  my $tableData = shift;  
  my $count = 1;
  my $results;
  for my $row (@$tableData) {
    if ($count > $ROWS_LIMIT) {
      $results .= makeLimitReached();
      last;
    }
    $results .= makeTableRow($count, $row);
    $count++;
  }
  if ($count == 1) {$results .= makeNoResultFound();}
  return $results;
}

sub fetchData {
  my $email = shift;
  my @result;
  my $select = "select created, str from log where address = :adr order by int_id, created desc limit ".($ROWS_LIMIT+1);
  my $sth = $dbh->prepare($select);
  $sth->bind_param(':adr', $email);
  $sth->execute;

  while (my @row = $sth->fetchrow_array) {
    push(@result, \@row);
  }

  return \@result;
}