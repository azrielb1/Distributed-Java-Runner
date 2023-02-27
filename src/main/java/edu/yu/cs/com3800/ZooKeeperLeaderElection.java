package edu.yu.cs.com3800;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState;

public class ZooKeeperLeaderElection {
    /**
     * time to wait once we believe we've reached the end of leader election.
     */
    private final static int finalizeWait = 200;

    /**
     * Upper bound on the amount of time between two consecutive notification
     * checks.
     * This impacts the amount of time to get the system up again after long
     * partitions. Currently 60 seconds.
     */
    private final static int maxNotificationInterval = 60000;

    private LinkedBlockingQueue<Message> incomingMessages;
    private ZooKeeperPeerServer myPeerServer;
    private Queue<Message> otherMessages = new LinkedList<>();

    private long proposedLeader;
    private long proposedEpoch;

    public ZooKeeperLeaderElection(ZooKeeperPeerServer server, LinkedBlockingQueue<Message> incomingMessages) {
        this.incomingMessages = incomingMessages;
        this.myPeerServer = server;
        if (server.getPeerState() == ServerState.OBSERVER) {
            this.proposedLeader = -1L;
            this.proposedEpoch = server.getPeerEpoch();
        } else {
            this.proposedLeader = server.getServerId();
            this.proposedEpoch = server.getPeerEpoch();
        }
    }

    private synchronized Vote getCurrentVote() {
        return new Vote(this.proposedLeader, this.proposedEpoch);
    }

    public synchronized Vote lookForLeader() {
        boolean enough = false;
        Map<Long, ElectionNotification> votesReceived = new HashMap<>();
        //send initial notifications to other peers to get things started
        sendNotifications();
        //Loop, exchanging notifications with other servers until we find a leader
        while (this.myPeerServer.getPeerState() == ServerState.LOOKING || this.myPeerServer.getCurrentLeader() == null) {
            Message m = null;
            try {
                long terminationTime = finalizeWait;
                //Remove next notification from queue, timing out after 2 times the termination time
                while ((m = incomingMessages.poll(terminationTime * 2, TimeUnit.MILLISECONDS)) == null) {
                    //if no notifications received..
                    Thread.sleep(terminationTime);
                    //..resend notifications to prompt a reply from others..
                    sendNotifications();
                    //.and implement exponential back-off when notifications not received.. 
                    terminationTime *= 2;
                    if (terminationTime > maxNotificationInterval) {
                        terminationTime = maxNotificationInterval;
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //if/when we get a message and it's from a valid server and for a valid server..
            if (isValidMessage(m)) {
                enough = false;
                ElectionNotification receivedNotification = getNotificationFromMessage(m);
                //keep track of the votes I received and who I received them from.
                //switch on the state of the sender:
                switch (receivedNotification.getState()) {
                    case LOOKING: //if the sender is also looking
                        votesReceived.put(receivedNotification.getSenderID(), receivedNotification);
                        //if the received message has a vote for a leader which supersedes mine, 
                        if (supersedesCurrentVote(receivedNotification.getProposedLeaderID(), receivedNotification.getPeerEpoch())) {
                            // change my vote and tell all my peers what my new vote is.
                            this.proposedLeader = receivedNotification.getProposedLeaderID();
                            this.proposedEpoch = receivedNotification.getPeerEpoch();
                            sendNotifications();
                        }
                        //if I have enough votes to declare my currently proposed leader as the leader:
                        if (haveEnoughVotes(votesReceived, getCurrentVote())) {
                            enough = true;
                            //first check if there are any new votes for a higher ranked possible leader before I declare a leader. If so, continue in my election loop
                            if (!incomingMessages.isEmpty()) break;
                            try {
                                Thread.sleep(finalizeWait); // wait once we believe we've reached the end of leader election
                            } catch (InterruptedException e) {
                            }
                            if (!incomingMessages.isEmpty()) break;
                            //If not, set my own state to either LEADING (if I won the election) or FOLLOWING (if someone else won the election) and exit the election
                            return acceptElectionWinner(getCurrentVote());
                        }
                        break;
                    case FOLLOWING: case LEADING: //if the sender is following a leader already or thinks it is the leader
                        votesReceived.put(receivedNotification.getSenderID(), receivedNotification);
                        //IF: see if the sender's vote allows me to reach a conclusion based on the election epoch that I'm in, i.e. it gives the majority to the vote of the FOLLOWING or LEADING peer whose vote I just received.
                        if (receivedNotification.getPeerEpoch() == proposedEpoch && haveEnoughVotes(votesReceived, receivedNotification)) {
                            //if so, accept the election winner.
                            //As, once someone declares a winner, we are done. We are not worried about / accounting for misbehaving peers.
                            return acceptElectionWinner(receivedNotification);
                        }
                        //ELSE: if n is from a LATER election epoch
                        else if (receivedNotification.getPeerEpoch() > proposedEpoch) {
                            //IF a quorum from that epoch are voting for the same peer as the vote of the FOLLOWING or LEADING peer whose vote I just received.
                            if (haveEnoughVotes(votesReceived, receivedNotification)) {
                                //THEN accept their leader, and update my epoch to be their epoch
                                return acceptElectionWinner(receivedNotification);
                            }
                            //ELSE:
                            //keep looping on the election loop.
                        }
                        break;
                    default:
                        break;
                }
            } else {
                otherMessages.add(m);
                if (enough) {
                    if (!incomingMessages.isEmpty()) continue;
                    try {
                        Thread.sleep(finalizeWait); // wait once we believe we've reached the end of leader election
                    } catch (InterruptedException e) {
                    }
                    if (!incomingMessages.isEmpty()) continue;
                    return acceptElectionWinner(getCurrentVote());
                }
            }
        }
        return getCurrentVote();
    }

    private void sendNotifications() {
        ElectionNotification n = new ElectionNotification(this.proposedLeader, myPeerServer.getPeerState(), myPeerServer.getServerId(), this.proposedEpoch);
        myPeerServer.sendBroadcast(MessageType.ELECTION, buildMsgContent(n));
    }

    private Vote acceptElectionWinner(Vote n) {
        // set my state to either LEADING or FOLLOWING
        myPeerServer.setPeerState(n.getProposedLeaderID() == myPeerServer.getServerId() ? ServerState.LEADING : ServerState.FOLLOWING);
        // clear out the incoming queue before returning
        incomingMessages.clear();
        incomingMessages.addAll(otherMessages);
        otherMessages.clear();
        return n;
    }

    /*
     * We return true if one of the following three cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        return (newEpoch > this.proposedEpoch) || ((newEpoch == this.proposedEpoch) && (newId > this.proposedLeader));
    }

    /**
     * Termination predicate. Given a set of votes, determines if have sufficient
     * support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one
     * current vote
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        // is the number of votes for the proposal >= the size of my peer server’s quorum?
        int quorumSize = myPeerServer.getQuorumSize();
        int count = 0;
        for (Vote v : votes.values()) {
            if (v.getProposedLeaderID() == proposal.getProposedLeaderID() && v.getPeerEpoch() == proposal.getPeerEpoch()) {
                count++;
            }
        }
        return count >= quorumSize;
    }

    private boolean isValidMessage(Message message) {
        return message.getMessageType() == MessageType.ELECTION;
    }

    /**
     * Turn a notification into a byte[] (for a message)
     * @param notification
     * @return
     */
    public static byte[] buildMsgContent(ElectionNotification notification) {
        /*
        size of buffer =
        1 long (proposed leader ID) = 8 bytes
        1 char (sender state) = 2 bytes
        1 long (sender ID) = 8 bytes
        1 long (peer epoch) = 8 bytes
        = 26
        */
        ByteBuffer buffer = ByteBuffer.allocate(26);
        buffer.clear();
        buffer.putLong(notification.getProposedLeaderID());
        buffer.putChar(notification.getState().getChar());
        buffer.putLong(notification.getSenderID());
        buffer.putLong(notification.getPeerEpoch());
        buffer.flip();
        return buffer.array();
    }

    /**
     * turn a received message into a notification object
     * @param received input message
     * @return the ElectionNotification contained in the message
     */
    public static ElectionNotification getNotificationFromMessage(Message received) {
        byte[] messageContents = received.getMessageContents();
        ByteBuffer buffer = ByteBuffer.wrap(messageContents);
        buffer.clear();
        long proposedLeaderID = buffer.getLong();
        ServerState state = ServerState.getServerState(buffer.getChar());
        long senderID = buffer.getLong();
        long peerEpoch = buffer.getLong();
        return new ElectionNotification(proposedLeaderID, state, senderID, peerEpoch);
    }
}
