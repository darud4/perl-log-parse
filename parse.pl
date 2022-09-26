use strict;
my $filename = shift || die "No file specified, exiting...";

open INF, $filename;
while (<INF>) {
  chomp;
  my ($date, $time, $id, $flag, $email, @rest) = split(/\s+/);
  
}
close INF;
