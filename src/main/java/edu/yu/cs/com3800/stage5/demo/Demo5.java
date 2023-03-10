package edu.yu.cs.com3800.stage5.demo;

import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.yu.cs.com3800.stage5.Gossiper;

import static edu.yu.cs.com3800.stage5.demo.NodeRunner.NODE_PORTS;
import static edu.yu.cs.com3800.stage5.demo.NodeRunner.GATEWAY_HTTP_PORT;

public class Demo5 {

    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final String javaCode = """
            public class HelloWorld {
                public String run() {
                    return "Hello from request number XXX";
                }
            }
            """;

    private static final ExecutorService executor = Executors.newCachedThreadPool();

    /**
     * This is the demo script for stage 5
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        // 2. Create a cluster of 7 nodes and one gateway, starting each in their own JVM
        Process[] nodes = new Process[8];
        for (int nodeId = 0; nodeId < 8; nodeId++) {
            nodes[nodeId] = new ProcessBuilder("java", "-cp", "target/classes", "edu.yu.cs.com3800.stage5.demo.NodeRunner", Integer.toString(nodeId)).inheritIO().start();
        }
        Thread.sleep(2000); // wait for nodes to start up

        // 3. Wait until the election has completed before sending any requests to the Gateway
        OptionalInt leaderId;
        while ((leaderId = getLeaderFromGateway()).isEmpty()) {
            Thread.sleep(250);
        }

        // 4. Once the gateway has a leader, send 9 client requests.
        List<Future<HttpResponse<String>>> responses = new ArrayList<>(9);
        responses.add(0, null);
        for (int i = 1; i <= 9; i++) {
            String request = javaCode.replace("XXX", Integer.toString(i));
            responses.add(i, executor.submit(new sendCompileAndRun(request)));
            System.out.println("Sending request " + i + ":\n" + request);
        }
        // wait to get all the responses and print them out
        for (int i = 1; i <= 9; i++) {
            var response = responses.get(i).get();
            System.out.println("Response " + i + ":\n\tCode: " + response.statusCode() + "\n\tBody: " + response.body());
        }

        // 5. kill -9 a follower JVM, printing out which one you are killing.
        int toKill = leaderId.getAsInt() == 3 ? 4 : 3;
        System.out.println("Killing follower ID " + toKill + "\n");
        nodes[toKill].destroyForcibly();
        Thread.sleep(Gossiper.CLEANUP); // wait for others to notice it is dead
        getLeaderFromGateway(); // retrieve and display the list of nodes from the Gateway. The dead node should not be on the list

        // 6. kill -9 the leader JVM and then pause 1000 milliseconds.
        toKill = leaderId.getAsInt();
        System.out.println("Killing leader ID " + toKill + "\n");
        nodes[toKill].destroyForcibly();
        Thread.sleep(1000);

        // Send/display 9 more client requests to the gateway, in the background
        for (int i = 10; i <= 18; i++) {
            String request = javaCode.replace("XXX", Integer.toString(i));
            responses.add(i, executor.submit(new sendCompileAndRun(request)));
            System.out.println("Sending request " + i + ":\n" + request);
        }

        // 7. Wait for the Gateway to have a new leader, and then print out the node ID of the leader.
        leaderId = null;
        Thread.sleep(Gossiper.CLEANUP); // wait for the gateway to notice it is dead (it wont have a new leader if it didn't notice yet)
        while ((leaderId = getLeaderFromGateway()).isEmpty()) {
            Thread.sleep(250);
        }
        
        // Print out the responses the client receives from the Gateway for the 9 requests sent in step 6. 
        for (int i = 10; i <= 18; i++) {
            var response = responses.get(i).get();
            System.out.println("Response " + i + ":\n\tCode: " + response.statusCode() + "\n\tBody: " + response.body());
        }

        // 8. Send/display 1 more client request (in the foreground), print the response
        String request = javaCode.replace("XXX", "19");
        System.out.println("Sending request 19:\n" + request);
        HttpResponse<String> response = new sendCompileAndRun(request).call();
        System.out.println("Response 19:\n\tCode: " + response.statusCode() + "\n\tBody: " + response.body());

        // 9. List the paths to files containing the Gossip messages received by each node.
        for (int port : NODE_PORTS) {
            System.out.println("./logs/failure_detection/Gossip-Verbose-on-port-" + port + ".log");
        }

        // 10. Shut down all the nodes
        for (int nodeId = 0; nodeId < 8; nodeId++) {
            nodes[nodeId].destroy();
        }
    }

    private static class sendCompileAndRun implements Callable<HttpResponse<String>> {
        private final String src;

        private sendCompileAndRun(String src) {
            this.src = src;
        }

        @Override
        public HttpResponse<String> call() throws Exception {
            URI uri = new URL("http", "localhost", GATEWAY_HTTP_PORT, "/compileandrun").toURI();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .setHeader("Content-type", "text/x-java-source")
                    .POST(BodyPublishers.ofString(src))
                    .build();
            return httpClient.send(request, BodyHandlers.ofString());
        }
    }

    /**
     * Asks the gateway for the id of the leader
     * @return the id of the leader if there is one
     */
    private static OptionalInt getLeaderFromGateway() throws Exception {
        System.out.println("Asking gateway for leader:");
        URI uri = new URL("http", "localhost", GATEWAY_HTTP_PORT, "/leader").toURI();
        HttpRequest request = HttpRequest.newBuilder(uri).build();
        HttpResponse<String> response = httpClient.send(request, BodyHandlers.ofString());
        if (response.statusCode() == 200) {
            System.out.println(response.body());
            return Arrays.stream(response.body().split("\n"))
                    .filter(line -> line.contains("LEADER"))
                    .map(line -> line.split(":")[0])
                    .mapToInt(Integer::parseInt)
                    .reduce((a, b) -> a);
        } else {
            return OptionalInt.empty();
        }
    }

}
