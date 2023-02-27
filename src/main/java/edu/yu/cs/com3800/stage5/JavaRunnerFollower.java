package edu.yu.cs.com3800.stage5;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;

public class JavaRunnerFollower extends Thread implements LoggingServer {

    private final ZooKeeperPeerServer peerServer;
    private ServerSocket tcpServer;
    private Logger logger;
    private Message queuedResponse;

    private final JavaRunner javaRunner = new JavaRunner();

    public JavaRunnerFollower(ZooKeeperPeerServer peerServer) throws IOException {
        this.peerServer = peerServer;
        this.setDaemon(true);
        setName("JavaRunnerFollower-udpPort-" + peerServer.getUdpPort());
    }

    public void shutdown() {
        if (tcpServer != null && !tcpServer.isClosed()) {
            try { tcpServer.close(); } catch (IOException e) {}
        }
        interrupt();
    }

    public Message getQueuedResponseAndShutdown() {
        this.shutdown();
        return this.queuedResponse;
    }

    @Override
    public void run() {
        this.logger = initializeLogging(JavaRunnerFollower.class.getCanonicalName() + "-on-port-" + peerServer.getUdpPort());
        this.logger.info("Begining role as follower");
        try {
            this.tcpServer = new ServerSocket(peerServer.getUdpPort() + 2);
        } catch (IOException e) {
            this.logger.log(Level.SEVERE, "Error opening socket", e);
        }
        queuedResponse = null;
        while (!this.isInterrupted()) {
            // Accept a tcp connection from the leader
            Socket socket = null;
            try {
                socket = tcpServer.accept();
            } catch (SocketException e) {
                this.logger.info("Socket closed while waiting for a connection.");
                break;
            } catch (IOException e) {
                this.logger.log(Level.SEVERE, "I/O error occured while waiting for a connection.", e);
            }
            if (socket == null) continue;

            // Read the message from the connection
            byte[] received = null;
            try {
                received = Util.readAllBytesFromNetwork(socket.getInputStream());
            } catch (IOException e) {
                this.logger.log(Level.SEVERE, "I/O error occured while reading from connection.", e);
            }
            Message message = new Message(received);
            InetSocketAddress sender = new InetSocketAddress(message.getSenderHost(), message.getSenderPort());
            this.logger.fine("Received message\n" + message);
            if (this.peerServer.isPeerDead(sender)) {
                // Ignore messages from peers marked as dead
                this.logger.fine("Ignoring message from dead peer: " + sender);
                continue;
            }

            Message response;
            switch (message.getMessageType()) {
                case NEW_LEADER_GETTING_LAST_WORK:
                    // If the leader is asking for the queued response, send it
                    if (queuedResponse != null) {
                        this.logger.finer("Leader requested last work. Sending response.");
                        response = new Message(MessageType.COMPLETED_WORK,
                                queuedResponse.getMessageContents(),
                                peerServer.getAddress().getHostString(),
                                peerServer.getUdpPort(),
                                message.getSenderHost(),
                                message.getSenderPort(),
                                queuedResponse.getRequestID(),
                                queuedResponse.getErrorOccurred());
                    } else {
                        // If there is no queued response, send an empty response (with a request ID of -1)
                        this.logger.finer("Leader " + sender +" requested last work, but there was none. Sending empty response.");
                        response = new Message(MessageType.COMPLETED_WORK,
                                "No result".getBytes(),
                                peerServer.getAddress().getHostString(),
                                peerServer.getUdpPort(),
                                message.getSenderHost(),
                                message.getSenderPort());
                    }
                    try {
                        socket.getOutputStream().write(response.getNetworkPayload());
                        this.logger.fine("Sent last work to " + socket.getInetAddress() + "\n\tResult: " + new String(response.getMessageContents()));
                    } catch (IOException e) {
                        this.logger.log(Level.SEVERE, "I/O error occured while sending last work", e);
                    }
                    queuedResponse = null;
                    break;
                case WORK:
                    // If it's a work message, have JavaRunner compile and run the code
                    String result;
                    boolean errorOccurred = false;
                    try {
                        result = javaRunner.compileAndRun(new ByteArrayInputStream(message.getMessageContents()));
                        if (result == null) {
                            result = "";
                        }
                        this.logger.fine("Successfully compiled and ran request ID: " + message.getRequestID());
                    } catch (Exception e) {
                        result = e.getMessage() + '\n' + Util.getStackTrace(e);
                        errorOccurred = true;
                        this.logger.fine("Failed to compile and run:\n\tRequest ID: " + message.getRequestID() + "\n\tError:" + e.getMessage());
                    }

                    // Respond with the result
                    response = new Message(MessageType.COMPLETED_WORK,
                            result.getBytes(),
                            peerServer.getAddress().getHostString(),
                            peerServer.getUdpPort(),
                            message.getSenderHost(),
                            message.getSenderPort(),
                            message.getRequestID(),
                            errorOccurred);
                    if (this.peerServer.isPeerDead(sender)) {
                        this.logger.finer("Leader died while running code. Queuing response.");
                        queuedResponse = response; // queue the response to be sent to the new leader
                    } else {
                        // If the leader is still alive, send the response
                        try {
                            socket.getOutputStream().write(response.getNetworkPayload());
                            this.logger.fine("Responded to " + socket.getInetAddress() + "\n\tResult: " + result);
                        } catch (IOException e) {
                            this.logger.log(Level.SEVERE, "I/O error occured while sending response", e);
                            queuedResponse = response;
                        }
                    }
                    break;
                default:
                    break;
            }
        }
    }

}
