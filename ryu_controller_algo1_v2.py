
from operator import attrgetter
import matplotlib.pyplot as plt
import networkx as nx
import time

from ryu import cfg
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.lib import hub
from ryu.ofproto import ofproto_v1_0

from ryu.lib.mac import haddr_to_bin
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet, arp, ipv4, tcp, udp, ether_types

from ryu.topology import api as topo_api
from ryu.topology import event as topo_event

from collections import defaultdict

CONF = cfg.CONF
FLOW_DEFAULT_PRIO_FORWARDING = 10           #   Priority for simple switch flow entries
FLOW_IPV4_PRIO = 100                        #   Priority for handle_ipv4 function flow entries
FLOW_DEFAULT_IDLE_TIMEOUT = 10000           #   Idle Timeout value for flows
CONGESTED_TO_UNCON_TIME = 3                 #   If the congested links become non-congested, the remote-control path returns back to original shortest path again.
MAX_BW = 1250000                            #   Maximum throughput capacity on the link
ENABLE_QoS = True                           #   Set QoS properties with boolean expression.
IS_CONGESTED_PERIOD = 1                     #   Congestion check period to check each link on the remote-control path.



#   Mininet mac and ip addresses of controller and robot
CONTROLLER_MAC = '00:00:00:00:00:01'
H1_MAC = '00:00:00:00:00:02'
H2_MAC = '00:00:00:00:00:03'
ROBOT_MAC = '00:00:00:00:00:04'

CONTROLLER_IP = '10.0.0.1'
ROBOT_IP = '10.0.0.4'



class PathCalculationError(Exception):
    pass


class QoS(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_0.OFP_VERSION]

    _CONTEXTS = {}

    def __init__(self, *args, **kwargs):
        super(QoS, self).__init__(*args, **kwargs)
        self.name = 'LoadBalancer'
        self.mac_to_port = {}
        self.ip_to_mac = {}
        # Variables for the network topology
        self.graph = nx.DiGraph()
        self.hosts = []
        self.links = []
        self.switches = []

        self.robot_is_running = False
        self.robot_to_controller = []       #   Robot to controller traffic
        self.controller_to_robot = []       #   Controller to Robot traffic
        self.con_to_noncon_flag = False     #   To add the path only one time.

        self.arp_checker = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: None)))

        self.update_topo_thread = hub.spawn(self._print_topo)
        self.update_link_load_thread = hub.spawn(self._poll_link_load)
        self.reset_arp_checker = hub.spawn(self._reset_arp)
        #   Another hub is created to check congestion link on the robot-control path.
        self.check_congested = hub.spawn(self._is_congested)

    @set_ev_cls(topo_event.EventHostAdd)
    def new_host_handler(self, ev):
        """
        A new host is added to the networkx graph.

        """
        host = ev.host

        self.logger.info("New %s detected", host)

        #   Add the new host with its MAC-address as a node to the graph.
        #   Add also appropriate edges to connect it to the next switch

        self.graph.add_node(
            host.mac,
            **{
                'type': 'client'
            }
        )
        if (host.mac, host.port.dpid) not in self.graph.edges:
            self.graph.add_edge(host.mac, host.port.dpid, **{'link_load': 0.0, 'time': 0.0, 'congested': False, 'non-congested_time': 0.0, 'weight': 0.0,
                                                             'old_num_bytes': 0, 'curr_speed': 1000000})
            self.graph.add_edge(host.port.dpid, host.mac, **{'port': host.port.port_no, 'link_load': 0.0, 'time': 0.0, 'congested': False, 'non-congested_time': 0.0, 'weight': 0.0,
                                                             'old_num_bytes': 0, 'curr_speed': 1000000})

    @set_ev_cls(topo_event.EventSwitchEnter)
    def new_switch_handler(self, ev):
        """
        A new switch is added to the networkx graph.
        """
        switch = ev.switch
        self.switches.append(switch)
        self.logger.info("New %s detected", switch)
        #   Add the new switch as a node to the graph.
        self.graph.add_node(switch.dp.id, **{'type': 'sw'})

    def __get_port_speed(self, dpid, port_no, switches_list):
        """
        Periodically returns the port speed info.
        """
        for switch in switches_list:
            if switch.dp.id == dpid:
                return switch.dp.ports[port_no].curr
        self.logger.debug("No BW info for %s at %s" % (port_no, dpid))
        return 1        #   default value

    @set_ev_cls(topo_event.EventLinkAdd)
    def new_link_handler(self, ev):
        """
        Add the new link as an edge to the graph
        """
        link = ev.link
        self.logger.info("New %s detected", link)
        if (link.src.dpid, link.dst.dpid) not in self.graph.edges:
            self.graph.add_edge(link.src.dpid, link.dst.dpid,
                                **{'port': link.src.port_no,
                                   'link_load': 0.0,
                                   'time': 0.0,
                                   'congested': False,
                                   'non-congested_time': 0.0,
                                   'weight': 0.0,
                                   'congested_pre': False,
                                   'congested_to_uncon': False,
                                   'old_num_bytes': 0,
                                   'curr_speed': self.__get_port_speed(link.src.dpid, link.src.port_no, self.switches)
                                   }
                                )
    def _is_congested(self):
        """
        Creates another hub and control congested links on the robot-control path periodically.
        Link control starts when the robot-control path is created. If there is a congested link on the robot-control path,
        calls _delete_robot_control_path_flows function to delete the flow entries.
        """
        hub.sleep(5)
        while True:
            if self.robot_is_running and ENABLE_QoS:
                #calculate the number of hops on robot-control path
                robot_control_path = self.robot_to_controller
                for link in range(len(robot_control_path)-1):
                    if self.graph[robot_control_path[link]][robot_control_path[link+1]]['congested']: #check here again.
                        self.logger.info('Congested path(robot to controller) between switch %s and %s' % (robot_control_path[link], robot_control_path[link+1]))
                        self._delete_robot_control_path_flows(robot_control_path)
                robot_control_path = self.controller_to_robot
                for link in range(len(robot_control_path)-1):
                    if self.graph[robot_control_path[link]][robot_control_path[link+1]]['congested']: #check here again.
                        self.logger.info('Congested path(controller to robot) between switch %s and %s' % (robot_control_path[link], robot_control_path[link+1]))
                        self._delete_robot_control_path_flows(robot_control_path)
            # Check congested links of robot_control path periodically for 1 sec
            hub.sleep(IS_CONGESTED_PERIOD)

    def _delete_robot_control_path_flows(self, path):
        """
        This function gets the requested path to delete the flow entries that matches with
        CONTROLLER_MAC and ROBOT_MAC
        """
        for datapath in (path):
            self.robot_is_running = False
            dp = topo_api.get_switch(self, datapath)[0].dp
            parser = dp.ofproto_parser
            ofproto = dp.ofproto
            command = ofproto.OFPFC_DELETE
            out_port = ofproto.OFPP_NONE
            #   Delete entries for Robot to Controller path
            if path == self.robot_to_controller:
                #create matching using Robot mac and Controller mac.
                match = parser.OFPMatch(dl_src = ROBOT_MAC, dl_dst = CONTROLLER_MAC)
                req = parser.OFPFlowMod(datapath = dp, match = match, command = command, out_port=out_port)
                dp.send_msg(req)

            #   Delete entries for Controller to Robot path
            else:
                match = parser.OFPMatch(dl_src = CONTROLLER_MAC, dl_dst=ROBOT_MAC)
                req = parser.OFPFlowMod(datapath = dp, match = match, command = command, out_port=out_port)
                dp.send_msg(req)


        self.logger.info("Deleted path %s " % (path))
        #   After the delete messages, clean the path from/to robot. and then stop checking(robot_is_running = False)
        #   until the new flow entry is installed to the switches
        #   Reset the flow path values.
        self.robot_to_controller = []
        self.controller_to_robot = []


    def _reset_arp(self):
        """
        Restart the arp_checker table periodically.
        """
        hub.sleep(2)
        while True:
            self.arp_checker = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: None)))
            hub.sleep(5)

    def _print_topo(self):
        """
        Prints a list of nodes and edges to the console
        Period 10s
        """
        hub.sleep(10)
        while True:
            self.logger.info("Nodes: %s" % self.graph.nodes)
            self.logger.info("Edges: %s" % self.graph.edges)
            robot_path_edges = []
            fixed_positions = {1: (3, 2), 2: (5, 2), 3: (7, 1), 4: (6, 0), 5: (4, 0), 6: (2,0), 7: (1,1)}
            labels = {1: 'S1', 2: 'S2', 3: 'S3', 4: 'S4', 5: 'S5', 6: 'S6', 7: 'S7'}

            if ROBOT_MAC in self.graph.nodes:
                fixed_positions[ROBOT_MAC] = (0, 1)
                labels[ROBOT_MAC] = 'R'
            if CONTROLLER_MAC in self.graph.nodes:
                fixed_positions[CONTROLLER_MAC] = (8, 1)
                labels[CONTROLLER_MAC] = 'C'
            if H1_MAC in self.graph.nodes:
                fixed_positions[H1_MAC] = (3, 2)
                labels[H1_MAC] = 'H1'
	    if H2_MAC in self.graph.nodes:
		fixed_positions[H2_MAC] = (5, 3)
		labels[H2_MAC] = 'H2'

            fixed_nodes = fixed_positions.keys()

            if len(self.controller_to_robot) > len(self.robot_to_controller):
                for i in range(len(self.controller_to_robot) - 1):
                    robot_path_edges.append((self.controller_to_robot[i], self.controller_to_robot[i + 1]))
            else:
                for i in range(len(self.robot_to_controller) - 1):
                    robot_path_edges.append((self.robot_to_controller[i], self.robot_to_controller[i + 1]))

            options_nodes = {
                'node_color': 'c',
                'node_size': 600
            }
            options1={
                'edge_color': 'green',
                'width': 1.0,
                'arrows': False
            }
            options2 = {
                'edge_color': 'red',
                'width': 3.0,
                'arrows': False
            }
            pos = nx.spring_layout(self.graph, pos=fixed_positions, fixed=fixed_nodes)
            nx.draw_networkx_nodes(self.graph, pos, **options_nodes)
            nx.draw_networkx_labels(self.graph, pos, labels=labels)
            nx.draw_networkx_edges(self.graph, pos, self.graph.edges, **options1)
            nx.draw_networkx_edges(self.graph, pos, robot_path_edges, **options2)

            plt.ion()
            plt.title("Current Topology")
            plt.show()
            plt.pause(0.001)
            hub.sleep(1)
            plt.clf()
            hub.sleep(2)

    def _poll_link_load(self):
        """
        Sends periodically port statistics requests to the SDN switches. Period: 1s
        """
        while True:
            for sw in self.switches:
                self._request_port_stats(sw.dp)
            hub.sleep(1)

    def _request_port_stats(self, datapath):
        """
        Send Port stats request to the datapath.
        """
        self.logger.info('send stats request: %016x', datapath.id)
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_NONE)
        datapath.send_msg(req)

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats_reply_handler(self, ev):
        """
        Calculates the link load based on the received port statistics. The values are stored as an attribute of the
        edges in the networkx DiGraph. [Bytes/Sec]/[Max Link Speed in Bytes]
        """
        body = ev.msg.body
        dpid = ev.msg.datapath.id

        out_str = ""
        for stat in sorted(body, key=attrgetter('port_no')):
            num_bytes = stat.rx_bytes + stat.tx_bytes
            new_time = time.time()
            #   Update the load value of the corresponding edge in self.graph
            for edge_candidate in self.graph[dpid]:
                self.logger.debug("%s %s %s" % (dpid, edge_candidate, self.graph[dpid][edge_candidate]))
                try:
                    if self.graph[dpid][edge_candidate]['port'] == stat.port_no:
                        delta_t = new_time - self.graph[dpid][edge_candidate]['time']
                        delta_bytes = num_bytes - self.graph[dpid][edge_candidate]['old_num_bytes']
                        speed = self.graph[dpid][edge_candidate]['curr_speed']
                        # load in bytes/sec
                        self.graph[dpid][edge_candidate]['link_load'] = 1.0 * delta_bytes / delta_t + 1
                        self.graph[dpid][edge_candidate]['time'] = new_time
                        self.graph[dpid][edge_candidate]['old_num_bytes'] = num_bytes
                        if self.graph[dpid][edge_candidate]['link_load'] > MAX_BW * 0.7:
                            # hold previous congested value to understand the congested -> non-congested phase
                            self.graph[dpid][edge_candidate]['congested_pre'] =  self.graph[dpid][edge_candidate]['congested']
                            self.graph[dpid][edge_candidate]['congested'] = True
                            self.graph[dpid][edge_candidate]['non-congested_time'] = 0
                            self.graph[dpid][edge_candidate]['congested_to_uncon'] = False
                            self.graph[dpid][edge_candidate]['weight'] += 1
                            self.con_to_noncon_flag = False

                        else:
                            self.graph[dpid][edge_candidate]['congested_pre'] = self.graph[dpid][edge_candidate]['congested']
                            self.graph[dpid][edge_candidate]['congested'] = False
                            # If a previously congested link becomes non-congested, set this variable to True.
                            if self.graph[dpid][edge_candidate]['congested_pre'] != self.graph[dpid][edge_candidate]['congested']:
                                self.graph[dpid][edge_candidate]['congested_to_uncon'] = True
                            if self.graph[dpid][edge_candidate]['congested_to_uncon']:
                                # If a previously congested link becomes non-congested for last CONGESTED_TO_UNCON_TIME seconds,
                                # delete all entries again.
                                if self.graph[dpid][edge_candidate]['non-congested_time'] == CONGESTED_TO_UNCON_TIME and not self.con_to_noncon_flag and ENABLE_QoS:
                                    if len(self.controller_to_robot)>0:
                                        self._delete_robot_control_path_flows(self.controller_to_robot)
                                    else :
                                        self._delete_robot_control_path_flows(self.robot_to_controller)
                                    self.con_to_noncon_flag = True
                                elif self.graph[dpid][edge_candidate]['non-congested_time'] == CONGESTED_TO_UNCON_TIME - 1 and ENABLE_QoS:
                                    self.graph[dpid][edge_candidate]['weight'] = 0
                                # Increase the non-congested time, if a previously congested links become non-congested.
                                self.graph[dpid][edge_candidate]['non-congested_time'] += 1

                        out_str += '%8x %s \t' % (stat.port_no, self.graph[dpid][edge_candidate]['link_load'])
                        out_str += '%8x %s \t' % (stat.port_no, self.graph[dpid][edge_candidate]['congested'])
                        break
                except KeyError:
                    pass

        self.logger.info('datapath %s' % dpid)
        self.logger.info('---------------- -------- '
                         '-------- -------- -------- '
                         '-------- -------- --------')
        self.logger.info(out_str)

    def calculate_path_to_server(self, src, dst, balanced):
        """
        Returns the path of the flow
        """
        path_out = []
        #   Use QoS path for both Robot and Robot controller
        if balanced:
            #   Load balanced routing for Robot
            path_tmp = nx.shortest_path(self.graph, src, dst, weight='weight')
            path_index = 0
            for dpid in path_tmp[:-1]:
                dp = topo_api.get_switch(self, dpid)[0].dp
                port = self.graph.edges[(dpid, path_tmp[path_index + 1])]['port']
                path_index += 1
                path_out.append({'dp': dp, 'port': port, 'dpid': dp.id})
        else:
            #   Determine path to destination using nx.shortest_path.
            path_tmp = nx.shortest_path(self.graph, src, dst, weight=None)
            path_index = 0
            for dpid in path_tmp[:-1]:
                dp = topo_api.get_switch(self, dpid)[0].dp
                port = self.graph.edges[(dpid, path_tmp[path_index + 1])]['port']
                path_index += 1
                path_out.append({'dp': dp, 'port': port, 'dpid': dp.id})
        self.logger.debug("Path: %s" % path_out)
        if len(path_out) == 0:
            raise PathCalculationError()
        return path_out

    def add_flow(self, datapath, priority, match, actions, buffer_id=None, idle_timeout=0):
        """
        Installs a single rule on a switch given the match and actions
        """
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        #   flag = 2 (OFPFF_CHECK_OVERLAP) to not install the same flows
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                    idle_timeout=idle_timeout, priority=priority, match=match,
                                    actions=actions, flags = 2)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                    idle_timeout=idle_timeout,
                                    match=match, actions=actions, flags = 2)
        datapath.send_msg(mod)

    def add_flow_for_path(self, parser, routing_path, pkt, dl_dst, dl_src, in_port):
        """
        Installs rules on all switches on the given path to forward the flow
        """

        port_previous_hop = in_port
        #   The switches between the incoming switch and the server
        for hop in (routing_path):
            self.logger.debug("previous port: %s, this hop dp: %s" % (port_previous_hop, hop['dp'].id))

            match = parser.OFPMatch(dl_src = dl_src, dl_dst = dl_dst,
                                        in_port=port_previous_hop)

            actions = [parser.OFPActionOutput(hop['port'], 0)]
            self.add_flow(hop['dp'], FLOW_IPV4_PRIO, match, actions, None, FLOW_DEFAULT_IDLE_TIMEOUT)
            port_previous_hop = hop['port']


    def _handle_ipv4(self, datapath, in_port, pkt):
        """
        Handles an IPv4 packet. Calculates the route and installs the appropriate rules. Finally, the packet is sent
        out at the target switch and port.
        """
        parser = datapath.ofproto_parser
        ofproto = datapath.ofproto

        #   extract headers from packet
        eth = pkt.get_protocol(ethernet.ethernet)
        ipv4_data = pkt.get_protocol(ipv4.ipv4)

        eth_dst_in = eth.dst
        eth_src = eth.src
        net_src = ipv4_data.src
        net_dst = ipv4_data.dst

        #do matching according to ethernet source and mac address
        if (net_src == ROBOT_IP and net_dst == CONTROLLER_IP) or (net_src == CONTROLLER_IP and net_dst == ROBOT_IP):
            #   Calculate QoS path the Homer.
            routing_path = self.calculate_path_to_server(
            datapath.id, self.ip_to_mac.get(net_dst, eth_dst_in), balanced=True)
            #   If the flow starts from Robot to Controller.
            if net_src == ROBOT_IP and net_dst == CONTROLLER_IP:
                for hop in routing_path:
                    if hop['dpid'] not in self.robot_to_controller:
                        self.robot_to_controller.append(hop['dpid'])
                self.robot_is_running = True
            #   If the flow starts from Controller to Robot.
            if net_src == CONTROLLER_IP and net_dst == ROBOT_IP:
                for hop in routing_path:
                    if hop['dpid'] not in self.controller_to_robot:
                        self.controller_to_robot.append(hop['dpid'])
                self.robot_is_running = True
            self.logger.info(
                    "Robot-Control Calculated path from %s-%s: %s" % (datapath.id, self.ip_to_mac.get(net_dst, eth_dst_in),
                                                        routing_path))
        else:
            #   Calculate shortest path for other traffics.
            routing_path = self.calculate_path_to_server(
                    datapath.id, self.ip_to_mac.get(net_dst, eth_dst_in), balanced=False)

            self.logger.info ("OTHER Calculated path from %s-%s: %s" % (datapath.id, self.ip_to_mac.get(net_dst, eth_dst_in),
                                                                 routing_path))

        self.add_flow_for_path(parser, routing_path, pkt, eth_dst_in, eth_src, in_port)
        self.logger.info("Installed flow entries FORWARDING (pub->priv)")

        actions_po = [parser.OFPActionOutput(routing_path[-1]["port"], 0)]
        out_po = parser.OFPPacketOut(datapath=routing_path[-1]['dp'],
                                     buffer_id=ofproto.OFP_NO_BUFFER,
                                    in_port=in_port, actions=actions_po, data=pkt.data)

        datapath.send_msg(out_po)
        self.logger.debug("Packet put out at %s %s", datapath, routing_path[-1]["port"])


        return False, pkt

    def _handle_simple_switch(self, datapath, in_port, pkt, buffer_id=None, eth_dst=None):
        """
        Simple learning switch handling for non IPv4 packets.
        """
        dpid = datapath.id
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        if buffer_id is None:
            buffer_id = ofproto.OFP_NO_BUFFER

        eth = pkt.get_protocols(ethernet.ethernet)[0]

        if eth_dst is None:
            eth_dst = eth.dst
        dl_src = eth.src
        if dpid not in self.mac_to_port:
            self.mac_to_port[dpid] = {}
        self.logger.debug("M2P: %s", self.mac_to_port)
        # learn mac address
        self.mac_to_port[dpid][dl_src] = in_port
        self.logger.debug("packet in %s %s %s %s", dpid, in_port, dl_src, eth_dst)
        if eth_dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][eth_dst]
        elif eth_dst == 'ff:ff:ff:ff:ff:ff':
            self.logger.info("Broadcast packet at %s %s %s", dpid, in_port, dl_src)
            out_port = ofproto.OFPP_FLOOD
        else:
            self.logger.debug("OutPort unknown, flooding packet %s %s %s %s", dpid, in_port, dl_src, eth_dst)
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        #   install a flow to avoid packet_in next time
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, dl_dst=haddr_to_bin(eth_dst))
            #   Verify if we have a valid buffer_id, if yes avoid to send both
            #   flow_mod & packet_out
            if buffer_id != ofproto.OFP_NO_BUFFER:
                self.add_flow(datapath, FLOW_DEFAULT_PRIO_FORWARDING, match, actions, buffer_id,
                              FLOW_DEFAULT_IDLE_TIMEOUT)
            else:
                self.add_flow(datapath, FLOW_DEFAULT_PRIO_FORWARDING, match, actions, None,
                              FLOW_DEFAULT_IDLE_TIMEOUT)
        data = None
        if buffer_id == ofproto.OFP_NO_BUFFER:
            data = pkt.data

        out = parser.OFPPacketOut(datapath=datapath, buffer_id=buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        """
        Is called if a packet is forwarded to the sdn-controller. Packet handling is done here.
        We drop LLDP and IPv6 packets and pre-install paths for IPv4 packets. Other packets are handled by simple learning switch
        """
        
        if ev.msg.msg_len < ev.msg.total_len:
            self.logger.debug("packet truncated: only %s of %s bytes",
                              ev.msg.msg_len, ev.msg.total_len)

        msg = ev.msg
        datapath = msg.datapath
        in_port = msg.in_port

        pkt = packet.Packet(msg.data)

        eth = pkt.get_protocol(ethernet.ethernet)
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            # ignore lldp packet
            return

        arp_header = pkt.get_protocol(arp.arp)
        ipv4_header = pkt.get_protocol(ipv4.ipv4)
        ipv6_header = 34525
        if arp_header:  
            # Learn src ip to mac mapping and forward
            if arp_header.src_ip not in self.ip_to_mac:
                self.ip_to_mac[arp_header.src_ip] = arp_header.src_mac
            eth_dst = self.ip_to_mac.get(arp_header.dst_ip, None)
            arp_dst = arp_header.dst_ip
            arp_src = arp_header.src_ip
            current_switch = datapath.id
            # Check if ARP-package from arp_src to arp_dst already passed this switch and drop or process accordingly
            if self.arp_checker[current_switch][arp_src][arp_dst]:
                self.logger.debug("ARP package known and therefore dropped")
                return
            else:
                self.arp_checker[current_switch][arp_src][arp_dst] = 1
                self.logger.debug("Forwarding ARP to learn address, but dropping all consecutive packages.")
                self._handle_simple_switch(datapath, in_port, pkt, msg.buffer_id, eth_dst)
        elif ipv4_header:  # IP packet -> load balanced routing
            self._handle_ipv4(datapath, in_port, pkt)
        elif ipv6_header:
            return
        else:
            self._handle_simple_switch(datapath, in_port, pkt, msg.buffer_id)
