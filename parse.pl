use strict;
use warnings;
use DBI;

my $config = readConfig();

my $dbh = DBI->connect("dbi:Pg:dbname=logparser", 'logparseruser', 'password4log', {AutoCommit => 0, RaiseError => 1});
my $filename = shift || 'out';

open INF, $filename;
open MESSAGE_INS, ">$filename.message.ins";
open LOG_INS, ">$filename.log.ins";
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
    print MESSAGE_INS makeInsert('message', 
      {
        created => $timestamp, 
        int_id => $messageId, 
        id => ($id = $1), 
        str => $messageBody
      });
  }
  unless ($id) {
    print LOG_INS makeInsert('log',
      {
        created => $timestamp,
        int_id => $messageId,
        str => $messageBody,
        address => $email
      }
    );
  }

}
close INF;
close ERR;
close MESSAGE_INS;
close LOG_INS;

sub makeInsert {
  my $tableName = shift;
  my $payload = shift;
  my @fields = keys %$payload;
  my @placeholders = map { ":$_" } keys %$payload;
  my $insert = "insert into $tableName (".join(', ', @fields).") values (".join (', ', @placeholders).")";
#  my $insert = "insert into $tableName ($fields) values ($values)";
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