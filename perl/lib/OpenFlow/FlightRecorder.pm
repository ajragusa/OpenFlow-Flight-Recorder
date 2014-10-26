package OpenFlow::FlightRecorder;

use strict;
use Net::OpenFlow;
use Net::OpenFlow::Protocol;
use Net::Pcap;
use XML::Simple;
use Log::Log4perl;
use MongoDB;
use Data::Dumper;
use Log::Log4perl;

sub new{
    my $that  = shift;
    my $class = ref($that) || $that;

    my %args = (
	config => '/etc/openflow_flight_recorder/config.xml',
	@_,
	);
    my $self = \%args;
    bless $self, $class;

    $self->{'logger'} = Log::Log4perl->get_logger('OpenFlow::FlightRecorder');
    $self->{'logger'}->info("Initializing OpenFlow Flight Recorder");

    $self->{'config'} = $self->_process_config();
    if(!defined($self->{'config'})){
	$self->{'logger'}->error("Error Processing Config");
	return;
    }
    
    $self->{'mongo'} = $self->_connect_to_mongo();
    
    if(!defined($self->{'mongo'})){
	$self->{'logger'}->error("Error connecting to Mongo");
    	return;
    }

    my $ofp = Net::OpenFlow::Protocol->new();
    $self->{'ofp'} = $ofp;
    if(!defined($self->{'ofp'})){
	$self->{'logger'}->error("Error creating OF Protocol parser");
    }

    return $self;
}

sub _connect_to_mongo{
    my $self = shift;

    my $client = MongoDB::MongoClient->new( host => $self->{'config'}->{'mongo'}->{'host'},
					    port => $self->{'config'}->{'mongo'}->{'port'});

    my $db = $client->get_database( $self->{'config'}->{'mongo'}->{'database'} );
    return $db;
}

sub _process_config{
    my $self = shift;
    
    my $xml =  XMLin($self->{'config'});
    $self->{'logger'}->info("Config: " . Data::Dumper::Dumper($xml));

    return $xml;
}

sub start_playback{
    my $self = shift;
    
}

sub start_capture{
    my $self = shift;
    my $error;
    my $cap = Net::Pcap::open_live( $self->{'config'}->{'capture'}->{'interface'},
				    9000,
				    1,
				    1000,
				    \$error);
    if(!defined($cap)){
	$self->{'logger'}->error("Error starting Packet Capture: " . $error);
    }
    my $user_data = {};
    Net::Pcap::loop($cap, 1, $self->_process_packet, $user_data);
    

}

sub _process_packet{
    my $self = shift;
    my ($user_data, $hdr, $pkt) = @_;
    $self->{'logger'}->info("Packet: " . Data::Dumper::Dumper($pkt));
  
    my $of_message;
    eval{
	$of_message = $self->{'ofp'}->ofpt_decode($pkt);
    }; 
    warn $@ if $@;

    if(defined($of_message)){
	$self->_push_to_mongo({ts => time(),
			       message => $of_message});
    }
}

sub _push_to_mongo{
    my $self = shift;
    my %params = @_;

    warn Data::Dumper::Dumper(%params);
    
}

1;
