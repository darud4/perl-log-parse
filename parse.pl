use strict;
use warnings;
use DBI;

my $config = readConfig();

my $dbh = DBI->connect("dbi:Pg:dbname=$config->{database}", $config->{username}, $config->{password}, 
  {AutoCommit => 0, RaiseError => 0});
my $filename = shift || 'out';

open INF, $filename;
open MESSAGE_ERR, ">$filename.message.err";
open LOG_ERR, ">$filename.log.err";
open ERR, ">$filename.err";

while (<INF>) {
  chomp;
  unless (/^\s*(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})\s(.*)$/) {
    print ERR "No timestamp found: $_\n";
    next;
  }
  
  my $timestamp = $1;
  my $messageBody = $2;
  my ($messageId, $flag, $email, $rest) = split(/\s+/, $messageBody, 4);
  my $id;
  if ($flag eq "<=" && $rest =~ /\bid=(\S+)\b/) { 
    makeInsert('message', 
      { created => $timestamp, int_id => $messageId, id => ($id = $1), str => $messageBody });
    if ($dbh->err) {print MESSAGE_ERR "Error inserting in message!\n";}  
  }
  unless ($id) {
    makeInsert('log',
      { created => $timestamp, int_id => $messageId, str => $messageBody, address => $email });
    if ($dbh->err) {print LOG_ERR "Error inserting in log!\n";}  
  }

}
close INF;
close ERR;
close MESSAGE_ERR;
close LOG_ERR;
$dbh->commit;
$dbh->disconnect;

sub makeInsert {
  my $tableName = shift;
  my $payload = shift;
  my @fields = keys %$payload;
  my @placeholders = map { ":$_" } keys %$payload;
  my $insert = "insert into $tableName (".join(', ', @fields).") values (".join (', ', @placeholders).")";
  my $sth = $dbh->prepare($insert);
  for (my $i=0; $i < scalar(@fields); $i++) {
    $sth->bind_param($placeholders[$i], $payload->{$fields[$i]});
  }
  $sth->execute();
  return $insert;
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