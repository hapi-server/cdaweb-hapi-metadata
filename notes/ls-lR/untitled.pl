#!/usr/bin/perl

undef $/;
$_ = <>;

for $match (split(/^total(.+)/o)) {
  print "---\n";
  print "match: $match";
  print "--\n";
  #my @spl = split(/total(.+)/, $match);
  #print @spl[0];
  #open(O, '>temp' . ++$n);
  #print O $match;
  #close(O);
}



if (0) {
use strict;
use warnings;

open (my $out, ">-") or die "oops";

while(<>) {
    if (m#^\./pub/data/(.+)/#o) {
      fpath = $1;
      if (m#^\.cdf$/#o) {
        print "$1\n";
        #close $out and open ($out, ">$1") or die "oops";
        next;
      }
    }

    #print $out $_
}
}