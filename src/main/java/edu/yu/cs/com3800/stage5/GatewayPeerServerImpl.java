package edu.yu.cs.com3800.stage5;

import java.net.InetSocketAddress;
import java.util.Map;

public class GatewayPeerServerImpl extends ZooKeeperPeerServerImpl {

    public GatewayPeerServerImpl(int udpPort, long peerEpoch, Long serverID, Map<Long, InetSocketAddress> peerIDtoAddress, int numberOfObservers) {
        super(udpPort, peerEpoch, serverID, peerIDtoAddress, serverID, numberOfObservers);
        super.setPeerState(ServerState.OBSERVER);
        super.setName("GatewayPeerServerImpl-udpPort-" + udpPort);
    }

    @Override
    public void setPeerState(ServerState newState) {
        // Do nothing
    }
}
