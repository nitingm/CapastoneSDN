from mininet.topo import Topo
from mininet.link import TCLink


class MyTopo(Topo):
    #"Simple topology example."

    def __init__(self):
        "Create custom topo."

        # Initialize topology
        Topo.__init__(self)

        # Add hosts and switches
        h1 = self.addHost('h1')  #    10.0.0.2
	h2 = self.addHost('h2')  #    10.0.0.3
        robot = self.addHost('robot')  #    10.0.0.4
        controller = self.addHost('controller')  #    10.0.0.1

        s1 = self.addSwitch('s1')

        s2 = self.addSwitch('s2')
        s3 = self.addSwitch('s3')
        s4 = self.addSwitch('s4')
        s5 = self.addSwitch('s5')
	s6 = self.addSwitch('s6')
	s7 = self.addSwitch('s7')
        # links between switches
        self.addLink(s1, s2, bw = 100)
        self.addLink(s2, s3, bw = 100)
        self.addLink(s3, s4, bw = 100)
        self.addLink(s4, s5, bw = 100)
	self.addLink(s5, s6, bw = 100)
	self.addLink(s6, s7, bw = 100)
        self.addLink(s1, s7, bw = 100)

        # links between hosts and switches
        self.addLink(s1, h1)
	self.addLink(s2, h2)
        self.addLink(s3, robot)
        self.addLink(s7, controller)

topos = {'mytopo': (lambda: MyTopo())}
