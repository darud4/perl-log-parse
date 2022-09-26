use strict;

my $dbh = 0;

my $NO_ID_TO_LOG = 0;

my $filename = shift || 'out';
open INF, $filename;
open MESSAGE_ERR, ">>$filename.message.err";
open MESSAGE_INS, ">>$filename.message.ins";
open LOG_ERR, ">>$filename.log.err";
open LOG_INS, ">>$filename.log.ins";

while (<INF>) {
  chomp;
  my ($date, $time, $messageId, $flag, $email, $rest) = split(/\s+/, $_, 6);
  my $id = undef;
  if ($flag eq "<=") {
    if ($rest =~ /\bid=(\S+)\b/) {
       writeMessage({
          timestamp => "$date $time", 
          messageId => $messageId, 
          id => ($id = $1), 
          message => "$messageId $flag $email $rest"
         });
    }
  }
  unless ($id) {writeLog(
    {timestamp => "$date $time",
     messageId => $messageId,
     message => "$messageId $flag $email $rest",
     address => $email
    }
  );}
}
close INF;

sub writeMessage {
  my $payload = shift;

  my $insert = "insert into messages (created, id, int_id, str) values ("
    .$payload->{timestamp}.", $payload->{id}, $payload->{messageId}, $payload->{message})";
  print MESSAGE_INS $insert."\n";
}

sub writeLog {
  my $payload = shift;

  my $insert = "insert into log (created, int_id, str, address) values ("
    .$payload->{timestamp}.", $payload->{messageId}, $payload->{message}, $payload->{address})";
  print LOG_INS $insert."\n";
}