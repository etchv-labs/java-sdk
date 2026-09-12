package com.etchv;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

final class AsyncTest {
  @Test void submissionsReturnReceiptsWithoutPolling() throws Exception {
    var failures = new CopyOnWriteArrayList<Throwable>();
    var calls = new CopyOnWriteArrayList<String>();
    var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    String webhook = "wh_" + "a".repeat(32);
    server.createContext("/", e -> {
      try {
        calls.add(e.getRequestURI().getPath());
        assertEquals("POST", e.getRequestMethod());
        assertTrue(e.getRequestURI().getPath().endsWith("/async"));
        assertEquals("webhook_id=" + webhook, e.getRequestURI().getQuery());
        assertEquals("stable_test_key", e.getRequestHeaders().getFirst("Idempotency-Key"));
        e.getRequestBody().readAllBytes();
        byte[] body = ("{\"status\":\"queued\",\"request_id\":\"req_" + "b".repeat(64) + "\"}").getBytes(StandardCharsets.UTF_8);
        e.sendResponseHeaders(202, body.length); e.getResponseBody().write(body);
      } catch (Throwable error) { failures.add(error); } finally { e.close(); }
    });
    server.start();
    try {
      var client = new EtchvClient("test-key", "http://127.0.0.1:" + server.getAddress().getPort(), Duration.ofSeconds(2));
      for (String media : List.of("images", "documents", "videos")) {
        var options = new EtchvClient.Options("file", "stable_test_key");
        assertEquals("queued", client.submitEmbed(media, new byte[]{1,2,3}, Map.of("asset", "test"), options, webhook).get("status").getAsString());
        assertEquals("queued", client.submitDetection(media, new byte[]{1,2,3}, options, webhook).get("status").getAsString());
      }
      assertEquals(6, calls.size()); assertTrue(failures.isEmpty(), failures.toString());
    } finally { server.stop(0); }
  }
}
