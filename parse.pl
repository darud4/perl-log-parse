#!/usr/bin/perl
use strict;
#use warnings;
use DBI;

# Если 1, то сообщения с флагом <=, у которых не найдена подстрока id=...., пишутся в таблицу log
# В этом случае сумма записей в двух таблицах будет равна количеству строк в файле, и каждая запись из файла попадет в одну из таблиц
# Если 0, то такие сообщения не пишутся никуда (в таблицу MESSAGE они не попадают, 
# так как поле id в ней обязательное, а у них его нет, а в таблицу LOG не попадают, 
# так как по условию записи с флагом "<=" в нее попадать не должны)
my $WRITE_NO_ID_MESSAGES_TO_LOG = 0;

# Если 1, то очищаем таблицы перед заливкой данных, если 0, то нет
my $PURGE_BEFORE_INSERT = 1;

# Имя файла лога по умолчанию
my $DEFAULT_LOG_FILE = 'out';

print "Reading configuration...";
my $config = readConfig();
print "done!\n";

print "Connecting to database...";
my $dbh = DBI->connect("dbi:Pg:dbname=$config->{database}", $config->{username}, $config->{password}, 
  {AutoCommit => 0, RaiseError => 0, PrintError => 0});
if ($dbh) {
  print "done!\n";
} else {
  print "ERROR!\n";
  exit;
}  

my $filename = shift || $DEFAULT_LOG_FILE;

print "Reading file $filename...\n";
open INF, $filename;
open MESSAGE_ERR, ">$filename.message.err";
open LOG_ERR, ">$filename.log.err";
open ERR, ">$filename.err";

my ($recordsAll, $recordsMessage, $recordsLog, $recordsError) = (0, 0, 0, 0); 

if ($PURGE_BEFORE_INSERT) {
  print "Purging tables...\n";
  purgeTable('message');
  purgeTable('log');
}

while (<INF>) {
  chomp;
  $recordsAll++;
  unless (/^\s*(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})\s(.*)$/) {
    print ERR "No timestamp found: $_\n";
    next;
  }
  
  my $timestamp = $1;
  my $messageBody = $2;
  my ($messageId, $flag, $email, $rest) = split(/\s+/, $messageBody, 4);
  my $id;
  if ($flag eq "<=" && $rest =~ /\bid=(\S+)\b/) { 
    $recordsMessage++;
    my $result = makeInsert('message', 
      { created => $timestamp, int_id => $messageId, id => ($id = $1), str => $messageBody });
    if (!$result) {print MESSAGE_ERR "Error inserting in message!".$dbh->errstr."\n$_\n";}  
  }
  if (($WRITE_NO_ID_MESSAGES_TO_LOG && !$id && $flag eq "<=") || ($flag ne '<=')) {
    $recordsLog++;
    my $result = makeInsert('log',
      { created => $timestamp, int_id => $messageId, str => $messageBody, address => $email });
    if (!$result) {print LOG_ERR "Error inserting in log!".$dbh->errstr."\n$_\n";}  
  }
  if ($dbh->err) {$recordsError++; print ERR $dbh->errstr."\n$_\n"; };
}
close INF;
close ERR;
close MESSAGE_ERR;
close LOG_ERR;

print "File processed, lines: $recordsAll\n";
print "Messages: $recordsMessage\n";
print "Logs: $recordsLog\n";
print "Errors: $recordsError\n";

$dbh->commit;
$dbh->disconnect;
print "Done!\n";

sub purgeTable {
  my $tableName = shift;
  my $delete = "delete from $tableName";
  my $sth = $dbh->do($delete);
}

sub makeInsert {
  my $result;
  my $tableName = shift;
  my $payload = shift;
  my @fields = keys %$payload;
  my @placeholders = map { ":$_" } keys %$payload;
  my $insert = "insert into $tableName (".join(', ', @fields).") values (".join (', ', @placeholders).")";
  my $sth = $dbh->prepare($insert);
  for (my $i=0; $i < scalar(@fields); $i++) {
    $sth->bind_param($placeholders[$i], $payload->{$fields[$i]});
  }
  return $sth->execute();
}

sub readConfig {
  open CONFIG, 'db.conf';
  my %config;
  while (<CONFIG>) {
    chomp;                  
    s/#.*//;                
    s/^\s+//;               
    s/\s+$//;               
    next unless length;     
    my ($key, $value) = split(/\s*=\s*/, $_, 2);
    $config{$key} = $value;
  }
  close CONFIG;
  return \%config;
}
