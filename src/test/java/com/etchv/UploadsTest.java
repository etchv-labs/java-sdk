package com.etchv;

import static org.junit.jupiter.api.Assertions.*;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

final class UploadsTest {
  private static final String ID = "ab".repeat(32);
  private static final String UPLOAD = "upl_" + "1".repeat(32);
  private static final byte[] PNG = png(300);

  private final List<Throwable> failures = new CopyOnWriteArrayList<>();
  private final List<String> calls = new CopyOnWriteArrayList<>();
  private HttpServer server;
  private String base;

  private interface Handler {
    void handle(HttpExchange exchange, String path, byte[] body) throws Exception;
  }

  private static byte[] png(int size) {
    byte[] bytes = new byte[size];
    System.arraycopy(new byte[] {(byte) 137, 80, 78, 71, 13, 10, 26, 10}, 0, bytes, 0, 8);
    return bytes;
  }

  private void serve(Handler handler) throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    base = "http://127.0.0.1:" + server.getAddress().getPort();
    server.createContext(
        "/",
        exchange -> {
          try {
            String path = exchange.getRequestURI().getPath();
            calls.add(exchange.getRequestMethod() + " " + path);
            handler.handle(exchange, path, exchange.getRequestBody().readAllBytes());
          } catch (Throwable error) {
            failures.add(error);
            exchange.sendResponseHeaders(500, -1);
          } finally {
            exchange.close();
          }
        });
    server.start();
  }

  @AfterEach
  void stop() {
    if (server != null) server.stop(0);
    assertTrue(failures.isEmpty(), failures.toString());
  }

  private String session(String kind) {
    return "{\"upload_id\":\"" + UPLOAD + "\",\"kind\":\"" + kind + "\",\"filename\":\"photo.png\",\"size\":"
        + PNG.length + ",\"status\":\"pending\",\"expires_at\":\"2026-10-10T00:00:00Z\",\"upload\":{\"method\":\"PUT\",\"url\":\""
        + base + "/put/" + kind + "/" + UPLOAD + ".png\",\"expires_at\":\"x\"}}";
  }

  private static void respond(HttpExchange e, int status, String contentType, byte[] body)
      throws IOException {
    if (contentType != null) e.getResponseHeaders().set("Content-Type", contentType);
    e.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
    if (body.length > 0) e.getResponseBody().write(body);
  }

  private static void json(HttpExchange e, int status, String body) throws IOException {
    respond(e, status, "application/json", body.getBytes(StandardCharsets.UTF_8));
  }

  private static void image(HttpExchange e) throws IOException {
    e.getResponseHeaders().set("X-Watermark-ID", ID);
    respond(e, 200, "image/png", PNG);
  }

  @Test
  void largeEmbedsUploadOnceWithoutTheApiKeyThenSendTheUploadId() throws Exception {
    serve(
        (e, path, body) -> {
          if (path.equals("/uploads")) {
            assertEquals("test-key", e.getRequestHeaders().getFirst("X-API-Key"));
            assertEquals(
                "{\"kind\":\"image\",\"filename\":\"photo.png\",\"size\":" + PNG.length + "}",
                new String(body, StandardCharsets.UTF_8));
            json(e, 201, session("image"));
          } else if (path.startsWith("/put/")) {
            assertNull(e.getRequestHeaders().getFirst("X-API-Key"));
            assertArrayEquals(PNG, body);
            respond(e, 200, null, new byte[0]);
          } else {
            String form = new String(body, StandardCharsets.UTF_8);
            assertTrue(form.contains("name=\"upload_id\"\r\n\r\n" + UPLOAD + "\r\n"));
            assertFalse(form.contains("name=\"file\""));
            assertTrue(form.contains("{\"recipient\":\"test\"}"));
            assertEquals("key-123456", e.getRequestHeaders().getFirst("Idempotency-Key"));
            image(e);
          }
        });
    try (var client = new EtchvClient("test-key", base, Duration.ofSeconds(10), 100)) {
      var result =
          client.embedImage(
              PNG, Map.of("recipient", "test"), new EtchvClient.Options("photo.png", "key-123456"));
      assertEquals(ID, result.watermarkId());
    }
    assertEquals(
        List.of(
            "POST /uploads", "PUT /put/image/" + UPLOAD + ".png", "POST /watermarks/images"),
        calls);
  }

  @Test
  void smallFilesStayInTheRequestBody() throws Exception {
    serve(
        (e, path, body) -> {
          assertTrue(new String(body, StandardCharsets.UTF_8).contains("name=\"file\""));
          image(e);
        });
    try (var client = new EtchvClient("test-key", base, Duration.ofSeconds(10))) {
      client.embedImage(PNG, Map.of("recipient", "test"), null);
    }
    assertEquals(List.of("POST /watermarks/images"), calls);
  }

  @Test
  void retriesResendTheSameUploadWithoutUploadingAgain() throws Exception {
    var puts = new AtomicInteger();
    var posts = new CopyOnWriteArrayList<String>();
    serve(
        (e, path, body) -> {
          if (path.equals("/uploads")) json(e, 201, session("image"));
          else if (path.startsWith("/put/")) {
            puts.incrementAndGet();
            respond(e, 200, null, new byte[0]);
          } else {
            posts.add(new String(body, StandardCharsets.UTF_8).contains(UPLOAD) ? UPLOAD : "file");
            if (posts.size() == 1) {
              e.getResponseHeaders().set("Retry-After", "0.01");
              json(e, 503, "{\"detail\":\"busy\"}");
            } else image(e);
          }
        });
    try (var client = new EtchvClient("test-key", base, Duration.ofSeconds(10), 100)) {
      client.embedImage(PNG, Map.of("recipient", "test"), null);
    }
    assertEquals(1, puts.get());
    assertEquals(List.of(UPLOAD, UPLOAD), posts);
  }

  @Test
  void largeSyncImageDetectionRunsAsAJob() throws Exception {
    String job = "req_" + "c".repeat(64);
    byte[] big = png(95 * 1024 * 1024 + 1);
    serve(
        (e, path, body) -> {
          if (path.equals("/uploads")) {
            assertTrue(new String(body, StandardCharsets.UTF_8).contains("\"kind\":\"detect\""));
            json(e, 201, session("detect"));
          } else if (path.startsWith("/put/")) respond(e, 200, null, new byte[0]);
          else if (path.equals("/watermarks/images/detect/async"))
            json(e, 202, "{\"request_id\":\"" + job + "\",\"status\":\"queued\"}");
          else
            json(
                e,
                200,
                "{\"watermarked\":true,\"confidence\":0.99,\"watermark_id\":\"" + ID + "\"}");
        });
    try (var client = new EtchvClient("test-key", base, Duration.ofSeconds(30))) {
      assertEquals(ID, client.detectImage(big, null).watermarkId());
    }
    assertEquals(
        List.of(
            "POST /uploads",
            "PUT /put/detect/" + UPLOAD + ".png",
            "POST /watermarks/images/detect/async",
            "GET /watermarks/detection-jobs/" + job + "/result"),
        calls);
  }

  @Test
  void limitsFollowTheOperation() throws Exception {
    try (var client = new EtchvClient("test-key")) {
      var embed =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  client.embedImage(
                      new byte[EtchvClient.MAX_FILE_SIZE + 1], Map.of("a", 1), null));
      assertTrue(embed.getMessage().contains("50 MB"));
      var detect =
          assertThrows(
              IllegalArgumentException.class,
              () -> client.detectImage(new byte[EtchvClient.MAX_DETECTION_FILE_SIZE + 1], null));
      assertTrue(detect.getMessage().contains("192 MB"));
      assertThrows(IllegalArgumentException.class, () -> client.uploadFile("audio", PNG, null));
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> new EtchvClient("test-key", EtchvClient.DEFAULT_BASE_URL, Duration.ofSeconds(1), 0));
  }

  @Test
  void aRefusedUploadRaisesWithItsStatus() throws Exception {
    serve(
        (e, path, body) -> {
          if (path.equals("/uploads")) json(e, 201, session("image"));
          else respond(e, 403, "text/plain", "Forbidden".getBytes(StandardCharsets.UTF_8));
        });
    try (var client = new EtchvClient("test-key", base, Duration.ofSeconds(10))) {
      var error =
          assertThrows(
              EtchvClient.EtchvException.class, () -> client.uploadFile("image", PNG, "photo.png"));
      assertEquals(403, error.statusCode());
    }
  }

  @Test
  void uploadFileReturnsTheReceivedSession() throws Exception {
    serve(
        (e, path, body) -> {
          if (path.equals("/uploads")) json(e, 201, session("image"));
          else respond(e, 200, null, new byte[0]);
        });
    try (var client = new EtchvClient("test-key", base, Duration.ofSeconds(10))) {
      var upload = client.uploadFile("image", PNG, "photo.png");
      assertEquals(
          new EtchvClient.UploadSession(
              UPLOAD, "image", "photo.png", PNG.length, "received", "2026-10-10T00:00:00Z"),
          upload);
    }
  }
}
