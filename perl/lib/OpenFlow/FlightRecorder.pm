package OpenFlow::FlightRecorder;

use strict;
use Net::OpenFlow;
use Net::OpenFlow::Protocol;
use NetPacket::Ethernet;
use NetPacket::IP;
use NetPacket::TCP;

use Net::Pcap;
use XML::Simple;
use Log::Log4perl;

use Net::RabbitFoot;
use JSON;

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
    
    $self->{'rabbit'} = $self->_connect_to_rabbit();
    
    if(!defined($self->{'rabbit'})){
	$self->{'logger'}->error("Error connecting to Rabbit");
    	return;
    }

    my $ofp = Net::OpenFlow::Protocol->new();
    $self->{'ofp'} = $ofp;
    if(!defined($self->{'ofp'})){
	$self->{'logger'}->error("Error creating OF Protocol parser");
    }

    $self->{'streams'};

    return $self;
}

sub _connect_to_rabbit{
    my $self = shift;
    
    my $rf = Net::RabbitFoot->new()->load_xml_spec()->connect( host => $self->{'config'}->{'rabbit'}->{'host'},
							       port => $self->{'config'}->{'rabbit'}->{'port'},
							       user => $self->{'config'}->{'rabbit'}->{'user'},
							       pass => $self->{'config'}->{'rabbit'}->{'pass'},
							       vhost => $self->{'config'}->{'rabbit'}->{'vhost'},
							       timeout => 1 );

    my $ch = $rf->open_channel();
    
	
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
#    $self->{'logger'}->debug("Header: " . Data::Dumper::Dumper($header));
#    $self->{'logger'}->info("Packet: " . Data::Dumper::Dumper($pkt));
  
    
    my $ethernet = NetPacket::Ethernet->decode($pkt);
    my $ip = NetPacket::IP->decode($ethernet->{'data'});
    my $tcp = NetPacket::TCP->decode($ip->{'data'});
    my $data = $tcp->{'data'};

    return if (!defined($data) || $data eq '');

    my $of_message;
    eval{
	$of_message = $self->{'ofp'}->ofpt_decode(\$data);
    }; 
#    warn $@ if $@;

#    warn Data::Dumper::Dumper($of_message);

    my $stream = $self->find_stream($ip);

    if(defined($of_message) && defined($of_message->{'ofp_header'})){
	$self->_push_to_rabbit({ts => $header->{'tv_sec'} . "." . $header->{'tv_usec'},
				src => $ip->{'src_ip'},
				dst => $ip->{'dest_ip'},
				stream => $stream,
				message => $of_message});
    }
}

sub find_stream{
    my $self = shift;
    my $ip = shift;
    my $tcp = shift;

    #we are looking for tcp streams!
    #first lok to see if we find our src ip and src port
    if(defined($self->{'streams'}->{$ip->{'src_ip'}})){
	if(defined($self->{'streams'}->{$ip->{'src_ip'}}->{$tcp->{'src_port'}})){
	    return $self->{'streams'}->{$ip->{'src_ip'}}->{$tcp->{'src_port'}};
	}
    }else{
	if(defined($self->{'streams'}->{$ip->{'dest_ip'}})){
	    if(defined($self->{'streams'}->{$ip->{'dest_ip'}}->{$tcp->{'dest_port'}})){
		return $self->{'streams'}->{$ip->{'dest_ip'}}->{$tcp->{'dest_port'}};
	    }
	}
    }
    
    #if we made it this far we didn't find the stream
    #set it up
    if(!defined($self->{'streams'}->{$ip->{'src_ip'}))){
	$self->{'streams'}->{$ip->{'src_ip'}} = {};
    }
    
    $self->{'streams'}->{$ip->{'src_ip'}}->{$tcp->{'src_port'}} = {};
    
    return $self->{'streams'}->{$ip->{'src_ip'}}->{$tcp->{'src_port'}};
	
    
}

sub _push_to_rabbit{
    my $self = shift;
    my $data = shift;

    my $json = to_json($data);

    $self->{'rabbit'}->publish( exchange    => "",
				routing_key => "OF_Messages",
				body        => $json,
				durable     => 0);
}

1;
