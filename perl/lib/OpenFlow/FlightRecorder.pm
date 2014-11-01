package OpenFlow::FlightRecorder;

use strict;
use Net::OpenFlow;
use Net::OpenFlow::Protocol;
use NetPacket::Ethernet;
use NetPacket::IP;
use NetPacket::TCP;
#use NetPacket::IPv6;
use Net::Pcap;
use XML::Simple;
use Log::Log4perl;
#use MongoDB;
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

#    my $client = MongoDB::MongoClient->new( host => $self->{'config'}->{'mongo'}->{'host'},
#					    port => $self->{'config'}->{'mongo'}->{'port'});
#
#    my $db = $client->get_database( $self->{'config'}->{'mongo'}->{'database'} );
#    return $db;
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
    my $promisc = 0;
    my $net;
    my $mask;
    Net::Pcap::lookupnet($self->{'config'}->{'capture'}->{'interface'}, \$net, \$mask, \$error);

    my $cap = Net::Pcap::open_live( $self->{'config'}->{'capture'}->{'interface'},
				    9000,
				    $promisc,
				    0,
				    \$error);

    if(!defined($cap)){
	$self->{'logger'}->error("Error starting Packet Capture: " . $error);
    }

    my $user_data = {ref => $self};
    

    my $filter_compiled;
    Net::Pcap::compile($cap, \$filter_compiled, "port " . $self->{'config'}->{'capture'}->{'port'}, 1, $net);
    Net::Pcap::setfilter($cap, $filter_compiled);

    Net::Pcap::loop($cap, -1, sub {
	my ($user_data, $header, $packet) = @_;
	$user_data->{'ref'}->_process_packet(header => $header, packet => $packet);
		    }, $user_data);
    

}

sub _process_packet{
    my $self = shift;
    my %params = @_;
    my $pkt = $params{'packet'};
    my $header = $params{'header'};


    #warn "Header: " . Data::Dumper::Dumper($header);
    #warn "Packet: " . Data::Dumper::Dumper($pkt);
    $self->{'logger'}->debug("Header: " . Data::Dumper::Dumper($header));
    $self->{'logger'}->info("Packet: " . Data::Dumper::Dumper($pkt));
  
    
    my $ethernet = NetPacket::Ethernet->decode($pkt);
    my $ip = NetPacket::IP->decode($ethernet->{'data'});
    my $tcp = NetPacket::TCP->decode($ip->{'data'});
    my $data = $tcp->{'data'};

    return if (!defined($data) || $data eq '');

#    warn "Data: " . $data . "\n";

    my $of_message;
    eval{
	$of_message = $self->{'ofp'}->ofpt_decode(\$data);
    }; 
    warn $@ if $@;

    warn Data::Dumper::Dumper($of_message);

    if(defined($of_message)){
	$self->_push_to_mongo({ts => time(),
			       message => $of_message});
    }
}

sub _push_to_mongo{
    my $self = shift;
    my %params = @_;

#    warn Data::Dumper::Dumper($params{'of_message'});
    
}

1;
