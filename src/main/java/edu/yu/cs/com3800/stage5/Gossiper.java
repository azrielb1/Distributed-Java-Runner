package edu.yu.cs.com3800.stage5;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.*;

import com.sun.net.httpserver.*;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;

public class Gossiper extends Thread implements LoggingServer {

    public static final int GOSSIP = 3000;
    public static final int FAIL = GOSSIP * 10;
    public static final int CLEANUP = FAIL * 2;

    private final ZooKeeperPeerServerImpl peerServer;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final Long id;
    private final Logger mainLogger;
    private final Logger summaryLogger;
    private final Logger verboseLogger;
    private long heartbeatCounter = 0;
    private Map<Long, HeartbeatData> heartbeatTable = new HashMap<>(); // maps each peer ID to its heartbeat data
    private final HttpServer httpServer;

    public Gossiper(ZooKeeperPeerServerImpl peerServer, LinkedBlockingQueue<Message> incomingMessages) throws IOException {
        this.peerServer = peerServer;
        this.id = peerServer.getServerId();
        this.incomingMessages = incomingMessages;
        this.setDaemon(true);
        int udpPort = peerServer.getUdpPort();
        setName("Gossiper-udpPort-" + udpPort);
        this.mainLogger = initializeLogging(Gossiper.class.getCanonicalName() + "-on-port-" + udpPort);

        // Set up summary logger
        this.summaryLogger = Logger.getLogger("Gossip-Summary-on-port-" + udpPort);
        summaryLogger.setUseParentHandlers(false);
        new File("./logs/failure_detection").mkdirs();
        FileHandler fh1;
        fh1 = new FileHandler("./logs/failure_detection/Gossip-Summary-on-port-" + udpPort + ".log");
        fh1.setLevel(Level.ALL);
        fh1.setFormatter(new SimpleFormatter());
        summaryLogger.addHandler(fh1);

        // Set up verbose logger
        this.verboseLogger = Logger.getLogger("Gossip-Verbose-on-port-" + udpPort);
        verboseLogger.setUseParentHandlers(false);
        FileHandler fh2;
        fh2 = new FileHandler("./logs/failure_detection/Gossip-Verbose-on-port-" + udpPort + ".log");
        fh2.setLevel(Level.ALL);
        fh2.setFormatter(new Formatter() {
            @Override
            public String format(LogRecord record) {
                return record.getMessage() + "\n";
            }
        });
        verboseLogger.addHandler(fh2);

        // Set up http service
        httpServer = HttpServer.create(new InetSocketAddress(udpPort + 4), 0);
        httpServer.createContext("/summary", new getLogHandler("./logs/failure_detection/Gossip-Summary-on-port-" + udpPort + ".log"));
        httpServer.createContext("/verbose", new getLogHandler("./logs/failure_detection/Gossip-Verbose-on-port-" + udpPort + ".log"));
        httpServer.start();
    }

    /**
     * Http handler for getting the log files
     */
    private class getLogHandler implements HttpHandler {
        private final Path logFile;

        public getLogHandler(String path) {
            this.logFile = Path.of(path);
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            mainLogger.fine("Recieved " + exchange.getRequestURI().getPath() + " HTTP " + exchange.getRequestMethod() + " request from " + exchange.getRemoteAddress());
            // check request method
            if (!exchange.getRequestMethod().equalsIgnoreCase("GET")) {
                // if the method is not GET, send a 405 response
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, 0);
                exchange.close();
                mainLogger.finer("Responded to " + exchange.getRemoteAddress() + " with HTTP 405 (Bad Method)");
                return;
            }

            byte[] fileBytes = Files.readAllBytes(logFile);

            exchange.getResponseHeaders().add("Content-Type", "text/x-log");
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, fileBytes.length);
            exchange.getResponseBody().write(fileBytes);
            exchange.close();
            mainLogger.finer("Responded to " + exchange.getRemoteAddress() + " with HTTP 200 (OK)");
        }
    }

    public void shutdown() {
        httpServer.stop(0);
        interrupt();
    }

    @Override
    public void run() {
        this.mainLogger.info("Starting Gossiper thread");
        while (!this.isInterrupted()) {
            long currentTime = System.currentTimeMillis();
            // increment our own heartbeat counter
            heartbeatTable.put(id, new HeartbeatData(heartbeatCounter++, currentTime));

            // Step 1) merge in to its records all new heartbeats / gossip info that the UDP receiver has
            mergeNewHeartbeats(currentTime);

            // Step 2) check for failures, using the records it has
            checkForFailures(currentTime);

            // Step 3) clean up old failures that have reached cleanup time
            cleanUpFailures(currentTime);

            // Step 4) gossip to a random peer
            sendGossip();

            // Step 5) sleep for the heartbeat/gossip interval
            try {
                Thread.sleep(GOSSIP);
            } catch (InterruptedException e) {
                this.mainLogger.info("Gossiper thread interrupted");
                break;
            }
        }
        this.mainLogger.info("Exiting Gossiper.run()");
    }

    private void mergeNewHeartbeats(long currentTime) {
        Queue<Message> otherMessages = new LinkedList<>();
        Message m = null;
        while ((m = incomingMessages.poll()) != null) { // For each received gossip message in the queue
            if (m.getMessageType() == MessageType.GOSSIP) {
                // deserialize the gossip message
                var receivedTable = deserializeHeartbeatTable(m.getMessageContents());
                String sender = m.getSenderHost() + ":" + m.getSenderPort();
                mainLogger.finest("Received heartbeat table from " + sender);
                saveToVerboseLog(sender, receivedTable, currentTime);
                // merge the received table into our own
                for (var newTableEntry : receivedTable.entrySet()) {
                    long receivedId = newTableEntry.getKey();
                    long receivedHeartbeat = newTableEntry.getValue().heartbeatCounter();
                    if (peerServer.isPeerDead(receivedId)) {
                        this.mainLogger.fine("Ignoring heartbeat from server " + newTableEntry.getKey() + " because it is dead");
                    } else if (!heartbeatTable.containsKey(receivedId)) { // If the peer is not in the table
                        heartbeatTable.put(receivedId, new HeartbeatData(receivedHeartbeat, currentTime)); // Add it
                        this.mainLogger.fine("Added new peer " + receivedId + " to heartbeat table");
                        this.summaryLogger.info(id + ": updated " + receivedId + "'s heartbeat sequence to " + receivedHeartbeat + " based on message from " + sender + " at node time " + currentTime);
                    } else if (receivedHeartbeat > heartbeatTable.get(receivedId).heartbeatCounter()) { // If the received heartbeat is newer
                        heartbeatTable.put(receivedId, new HeartbeatData(receivedHeartbeat, currentTime)); // Update it in the table
                        this.summaryLogger.info(id + ": updated " + receivedId + "'s heartbeat sequence to " + receivedHeartbeat + " based on message from " + sender + " at node time " + currentTime);
                    }
                }
            } else {
                // It is not a gossip messege
                otherMessages.add(m);
            }
        }
        incomingMessages.addAll(otherMessages);
    }

    private void checkForFailures(long currentTime) {
        for (var entry : heartbeatTable.entrySet()) {
            if (peerServer.isPeerDead(entry.getKey())) {
                continue;
            }
            if (currentTime - entry.getValue().time() > FAIL) {
                this.mainLogger.info("Peer " + entry.getKey() + " has failed");
                this.summaryLogger.info(id + ": no heartbeat from server " + entry.getKey() + " - SERVER FAILED");
                System.out.println(id + ": no heartbeat from server " + entry.getKey() + " - SERVER FAILED");
                peerServer.reportFailedPeer(entry.getKey(), summaryLogger);
            }
        }
    }

    private void cleanUpFailures(long currentTime) {
        final var iterator = heartbeatTable.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            if (currentTime - entry.getValue().time() > CLEANUP) {
                iterator.remove();
                this.mainLogger.fine("Peer " + entry.getKey() + " has been removed from the heartbeat table (Cleanup)");
            }
        }
    }

    private void sendGossip() {
        InetSocketAddress randomPeer = peerServer.getRandomPeer();
        peerServer.sendMessage(MessageType.GOSSIP, serializeHeartbeatTable(), randomPeer);
        this.mainLogger.finest("Sent gossip message to " + randomPeer);
    }

    private record HeartbeatData(long heartbeatCounter, long time) implements Serializable {}

    private byte[] serializeHeartbeatTable() {
        // remove dead peers from the table before sending
        var tableToSend = new HashMap<>(heartbeatTable);
        for (long id : heartbeatTable.keySet()) {
            if (peerServer.isPeerDead(id)) {
                tableToSend.remove(id);
            }
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            new ObjectOutputStream(baos).writeObject(tableToSend);
        } catch (Exception e) {
            mainLogger.log(Level.SEVERE, "Error serializing heartbeat table", e);
        }
        return baos.toByteArray();
    }

    @SuppressWarnings("unchecked")
    private HashMap<Long, HeartbeatData> deserializeHeartbeatTable(byte[] data) {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        try {
            return (HashMap<Long, HeartbeatData>) new java.io.ObjectInputStream(bais).readObject();
        } catch (Exception e) {
            mainLogger.log(Level.SEVERE, "Error deserializing heartbeat table", e);
        }
        return null;
    }

    private void saveToVerboseLog(String sender, HashMap<Long, HeartbeatData> receivedTable, long currentTime) {
        String time = new SimpleDateFormat("MM/dd/yyyy, HH:mm:ss.S").format(new Date(currentTime));
        this.verboseLogger.info("Message from " + sender + " received at " + time + ":");
        this.verboseLogger.info(receivedTable.toString());
    }

}
