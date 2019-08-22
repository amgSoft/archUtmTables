#!/usr/bin/perl

use DBI;

$instance = join ';','DBI:mysql:database=UTM5:127.0.0.1','mysql_read_default_group=perl','mysql_read_default_file=/etc/my.cnf';
$db_user = 'dbuser';
$db_pass = 'dbpass';
#$db_name = 'UTM5';

$db_arc_name = 'UTM5arh';
#$tbl_count = 7;
$tbl_count = 4;
@start_date;

#@tbl_names = ('discount_transactions_all','discount_transactions_iptraffic_all','tel_sessions_log','tel_sessions_detail','dhs_sessions_log','dhs_sessions_detail','payment_transactions');
@tbl_names = ('discount_transactions_all','dhs_sessions_log','dhs_sessions_detail','payment_transactions');
#@tbl_types = (1,2,3,4,5,6,7);
@tbl_types = (1,5,6,7);
#@tbl_datefields = ('discount_date','discount_date','recv_date','recv_date','recv_date','recv_date','payment_enter_date');
@tbl_datefields = ('discount_date','recv_date','recv_date','payment_enter_date');

#@tbl_shortnames = ('dta','dti','tsl','tsd','dsl','dsd','ptr');
@tbl_shortnames = ('dta','dsl','dsd','ptr');

$dbh = DBI->connect($instance,$db_user,$db_pass,{ RaiseError => 1}) or die "\nConnection failed...\nError:$DBI::errstr\n";

$sth = $dbh->prepare ("SELECT YEAR(NOW())");
$sth->execute ();
$y = ($sth->fetchrow_array ())[0];

$sth = $dbh->prepare ("SELECT MONTH(NOW())");
$sth->execute ();
$m = ($sth->fetchrow_array ())[0];

$sth = $dbh->prepare ("SELECT UNIX_TIMESTAMP('$y-$m-01 0:00:00')");
$sth->execute ();
$end_date = ($sth->fetchrow_array ())[0];
            
$m--;

if ($m == 0) {
    $m = 12;
    $y--;
}
$suffix = sprintf ("%04d_%02d",$y,$m);
    
#$sth = $dbh->prepare ("SELECT UNIX_TIMESTAMP('$y-$m-01 0:00:00')");
#$sth->execute ();
#$start_date = ($sth->fetchrow_array ())[0];

#Modified
for($i = 0; $i < $tbl_count; $i++){
    $sth = $dbh->prepare ("SELECT MIN($tbl_datefields[$i]) FROM $tbl_names[$i]");
    $sth->execute ();
    $start_date[$i] = ($sth->fetchrow_array ())[0];    
}

$sth = $dbh->prepare ("SELECT MAX(archive_id) FROM archives");
$sth->execute ();
$arc_id = ($sth->fetchrow_array ())[0] + 1;

undef $sth;

for ($j = 0;$j < $tbl_count;$j++) {
    $tbl_orig = $tbl_names[$j];
    $tbl_shortname = $tbl_shortnames[$j];
    $tbl_bkp = 'arc_' . $tbl_shortname;
    $datefield = $tbl_datefields[$j];
    $tbl_type = $tbl_types[$j];
    $full_arcname = $db_arc_name . "." . $tbl_shortname . "_" . $suffix;
#print $full_arcname . "\n";

        print "Processing $tbl_orig\n";
        $dbh->do ("ALTER TABLE $tbl_orig RENAME TO $tbl_bkp");
        $dbh->do ("CREATE TABLE $tbl_orig LIKE $tbl_bkp");
        $dbh->do ("INSERT INTO $tbl_orig SELECT * FROM $tbl_bkp WHERE $datefield>=$end_date");
        $dbh->do ("DELETE FROM $tbl_bkp WHERE $datefield>=$end_date");
        $dbh->do ("ALTER TABLE $tbl_bkp ENGINE=MyISAM");
        $dbh->do ("CREATE TABLE $full_arcname LIKE $tbl_bkp");
        $dbh->do ("ALTER TABLE $full_arcname ENGINE=MyISAM");
        $dbh->do ("INSERT INTO $full_arcname SELECT * FROM $tbl_bkp");
        $dbh->do ("DROP TABLE $tbl_bkp"); $end_date--;
        $dbh->do ("INSERT INTO archives (archive_id,table_type,table_name,start_date,end_date) VALUES ('$arc_id','$tbl_type','$full_arcname','$start_date[$j]','$end_date')");
        $end_date++;
}
$dbh->disconnect ();
