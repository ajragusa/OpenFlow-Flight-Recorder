#!/usr/bin/perl

#OpenFlow Flight Recorder

use strict;
use Log::Log4perl;
use OpenFlow::FlightRecorder;


sub main{
    my $logger = Log::Log4perl::init('/etc/openflow_flight_recorder/logger.conf');
    my $offr = OpenFlow::FlightRecorder->new( config => "/etc/openflow_flight_recorder/config.xml");
    $offr->start_capture();
}

main();
