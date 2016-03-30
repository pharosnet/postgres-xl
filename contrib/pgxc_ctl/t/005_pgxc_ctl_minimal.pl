use strict;
use warnings;
use Cwd;
use Config;
use TestLib;
use Test::More tests => 6;

program_help_ok('pgxc_ctl');
program_version_ok('pgxc_ctl');

my $dataDirRoot="~/DATA/pgxl/nodes/";
my $pgxcCtlRoot="~/pgxc_ctl/";


system_or_bail 'pgxc_ctl', 'clean', 'all' ;
#delete related dirs for cleanup
my $result = system("rm -rf $dataDirRoot");
my $result = system("rm -rf $pgxcCtlRoot");

system_or_bail 'pgxc_ctl', 'prepare', 'minimal' ;

system_or_bail 'pgxc_ctl', 'init', 'all' ;

system_or_bail 'pgxc_ctl', 'monitor', 'all' ;

#add cleanup
system_or_bail 'pgxc_ctl', 'clean', 'all' ;

#delete related dirs for cleanup
my $result = system("rm -rf $dataDirRoot");
my $result = system("rm -rf $pgxcCtlRoot");
