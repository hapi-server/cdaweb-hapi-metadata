#!/usr/bin/perl

undef $/;
$_ = <>;
$n = 0;

for $match (split(/\nRate: (.+)/)) {
print "match: $match";
print "\n---";
#  open(O, '>temp' . ++$n);
#  print O $match;
#  close(O);
}