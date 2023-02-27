package edu.yu.cs.com3800.stage5;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.*;
import java.net.http.*;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState;

public class Stage5Test {
    private static final int NUM_REQUESTS = 20;

    /** Gateway peer server is index 0 */
    private static final int[] PEER_SERVER_PORTS = { 8000, 8010, 8020, 8030, 8040, 8050, 8060, 8070 };
    private static final int GATEWAY_HTTP_PORT = 8001;

    private final HttpClient httpClient = HttpClient.newHttpClient();
    ExecutorService executor = Executors.newCachedThreadPool();
    private static GatewayServer gateway;
    private static ArrayList<ZooKeeperPeerServer> servers;

    @BeforeAll
    @SuppressWarnings("unchecked")
    static void createAndStartServers() throws IOException {
        // step 1: create IDs and addresses
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(8);
        for (int i = 0; i < PEER_SERVER_PORTS.length; i++) {
            peerIDtoAddress.put((long) i, new InetSocketAddress("localhost", PEER_SERVER_PORTS[i]));
        }

        // step 2: create and start servers
        servers = new ArrayList<>(3);
        for (long i = 1; i < PEER_SERVER_PORTS.length; i++) {
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            var address = map.remove(i);
            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(address.getPort(), 0, i, map, 0L, 1);
            servers.add(server);
            server.start();
        }

        // step 3: create and start gateway
        HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
        var address = map.remove(0L);
        gateway = new GatewayServer(GATEWAY_HTTP_PORT, address.getPort(), 0L, map, 1);
        gateway.start();
    }

    @AfterAll
    static void shutdownServers() {
        for (ZooKeeperPeerServer server : servers) {
            server.shutdown();
        }
        gateway.stop();
    }

    @Test
    void testOneRequest() throws Exception {
        System.out.println("Running testOneRequest");
        String src = """
                import java.util.ArrayList;
                import java.util.Collections;
                import java.util.List;

                public class TestClass {
                    public String run() {
                        List<String> list = new ArrayList<>();
                        list.add("i");
                        list.add("am");
                        list.add("adding");
                        list.add("some");
                        list.add("random");
                        list.add("words");
                        Collections.sort(list);
                        return list.get(0);
                    }
                }
                """;
        String expected = "adding";

        // step 1: send request to the gateway
        HttpResponse<String> r = new sendHttpRequest(src).call();
        assertEquals(200, r.statusCode());
        assertEquals(expected, r.body());
        System.out.println("testOneRequest passed");
    }

    @Test
    public void testConcurrentRequests() throws InterruptedException, ExecutionException {
        System.out.println("Running testConcurrentRequests");
        String src = """
            public class HelloWorld {
                public String run() {
                    return "Hello " + "world! " + XXX;
                }
            }
        """;

        // step 1: send requests to the gateway
        List<Future<HttpResponse<String>>> responses = new ArrayList<>(NUM_REQUESTS);
        for (int i = 0; i < NUM_REQUESTS; i++) {
            responses.add(i, executor.submit(new sendHttpRequest(src.replace("XXX", ""+i))));
        }

        // step 2: validate responses from gateway
        for (int i = 0; i < NUM_REQUESTS; i++) {
            HttpResponse<String> r = responses.get(i).get();
            assertEquals(200, r.statusCode());
            assertEquals("Hello world! " + i, r.body());
        }
        System.out.println("testConcurrentRequests passed");
    }

    @Test
    void testBadRequests() throws InterruptedException, ExecutionException {
        System.out.println("Running testBadRequests");
        // step 1: send bad requests to the gateway
        List<Future<HttpResponse<String>>> responses = new ArrayList<>(3);

        // no run method
        responses.add(executor.submit(new sendHttpRequest("""
                public class HelloWorld {
                    public static void main(String[] args) {
                        System.out.println("Hello World");
                    }
                }
            """)));

        // constructor takes arguments
        responses.add(executor.submit(new sendHttpRequest("""
            public class HelloWorld {
                public HelloWorld(int i) {
                }
                public String run() {
                    return "Hello " + "World " + (40 + 14 / 7);
                }
            }
        """)));

        // missing semicolon
        responses.add(executor.submit(new sendHttpRequest("""
            public class HelloWorld {
                public String run() {
                    return "Hello World "
                }
            }
        """)));

        // step 2: validate bad responses from gateway
        for (Future<HttpResponse<String>> response : responses) {
            HttpResponse<String> r = response.get();
            assertEquals(400, r.statusCode());
            assertFalse(r.body().isBlank());
        }
        System.out.println("testBadRequests passed");
    }

    @Test
    void testLogEndpoints() throws InterruptedException, ExecutionException, MalformedURLException, URISyntaxException {
        System.out.println("Running testLogEndpoints");
        // Create requests
        List<URI> uris = new ArrayList<>();
        for (var server : servers) {
            int port = server.getUdpPort();
            uris.add(new URL("http", "localhost", port+4, "/summary").toURI());
            uris.add(new URL("http", "localhost", port+4, "/verbose").toURI());
        }

        // Send requests
        List<Future<HttpResponse<String>>> responses = new ArrayList<>();
        for (URI uri : uris) {
            HttpRequest request = HttpRequest.newBuilder(uri).build();
            responses.add(executor.submit(() -> httpClient.send(request, BodyHandlers.ofString())));
        }

        // Validate responses
        for (Future<HttpResponse<String>> response : responses) {
            HttpResponse<String> r = response.get();
            assertEquals(200, r.statusCode());
        }
        System.out.println("testLogEndpoints passed");
    }

    @Test
    void testFailureDetection() {
        System.out.println("Running testFailureDetection");
        // step 1: kill a server
        ZooKeeperPeerServer toKill = servers.remove(0);
        if (toKill.getPeerState() == ServerState.LEADING) {
            servers.add(toKill);
            toKill = servers.remove(0);
        }
        toKill.shutdown();

        // step 2: wait for everyone to notice
        try {
            Thread.sleep(Gossiper.CLEANUP);
        } catch (InterruptedException e) {
            return;
        }

        // step 3: validate that everyone knows the server is dead
        for (var server : servers) {
            assertTrue(server.isPeerDead(toKill.getServerId()));
        }
        System.out.println("testFailureDetection passed");
    }

    private class sendHttpRequest implements Callable<HttpResponse<String>> {
        private final String src;

        private sendHttpRequest(String src) {
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
}
