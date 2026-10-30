package com.etchv;

import static org.junit.jupiter.api.Assertions.*;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Dropped connections: instant detection, downloads and batch archives. */
final class RetriesTest {
  private static final String ID = "ab".repeat(32);
  private static final String ASSET = "ast_" + "a".repeat(64);
  private static final String DELIVERY = "std_" + "d".repeat(64);
  private static final String BATCH = "bat_" + "b".repeat(32);
  private static final byte[] PNG = png(300);
  private static final String DETECTED =
      "{\"watermarked\":true,\"confidence\":0.99,\"watermark_id\":\"" + ID + "\"}";

  private final List<Throwable> failures = new CopyOnWriteArrayList<>();
  private final List<String> calls = new CopyOnWriteArrayList<>();
  private HttpServer server;
  private String base;

  @TempDir java.nio.file.Path temp;

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
            var uri = exchange.getRequestURI();
            calls.add(
                exchange.getRequestMethod()
                    + " "
                    + uri.getPath()
                    + (uri.getQuery() == null ? "" : "?" + uri.getQuery()));
            handler.handle(exchange, uri.getPath(), exchange.getRequestBody().readAllBytes());
          } catch (Throwable error) {
            failures.add(error);
            exchange.sendResponseHeaders(500, -1);
          } finally {
            exchange.close(); // After a deliberately short body, this closes the connection.
          }
        });
    server.start();
  }

  @AfterEach
  void stop() {
    if (server != null) server.stop(0);
    assertTrue(failures.isEmpty(), failures.toString());
  }

  private EtchvClient client() {
    return new EtchvClient("test-key", base, Duration.ofSeconds(10));
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

  /** Declares {@code body} but sends only {@code sent} bytes of it, as a reset connection does. */
  private static void drop(HttpExchange e, byte[] body, int sent) throws IOException {
    e.sendResponseHeaders(200, body.length);
    e.getResponseBody().write(body, 0, sent);
    e.getResponseBody().flush();
  }

  private static String conflict(String code) {
    return "{\"detail\":{\"code\":\"" + code + "\",\"message\":\"" + code + "\"}}";
  }

  @Test
  void aDroppedDetectionIsResentWithTheSameGeneratedKey() throws Exception {
    var keys = new CopyOnWriteArrayList<String>();
    serve(
        (e, path, body) -> {
          keys.add(e.getRequestHeaders().getFirst("Idempotency-Key"));
          byte[] result = DETECTED.getBytes(StandardCharsets.UTF_8);
          if (keys.size() % 2 == 1) drop(e, result, 5);
          else respond(e, 200, "application/json", result);
        });
    try (var client = client()) {
      assertEquals(ID, client.detectImage(PNG, null).watermarkId());
      assertEquals(ID, client.detectDocument(PNG, null).watermarkId());
    }
    assertEquals(4, keys.size());
    assertNotNull(keys.get(0));
    assertEquals(keys.get(0), keys.get(1));
    assertEquals(keys.get(2), keys.get(3));
    assertNotEquals(keys.get(0), keys.get(2));
    assertEquals(
        List.of(
            "POST /watermarks/images/detect",
            "POST /watermarks/images/detect",
            "POST /watermarks/documents/detect",
            "POST /watermarks/documents/detect"),
        calls);
  }

  @Test
  void aGivenKeyIsKept() throws Exception {
    var keys = new CopyOnWriteArrayList<String>();
    serve(
        (e, path, body) -> {
          keys.add(e.getRequestHeaders().getFirst("Idempotency-Key"));
          byte[] result = DETECTED.getBytes(StandardCharsets.UTF_8);
          if (keys.size() == 1) drop(e, result, 5);
          else respond(e, 200, "application/json", result);
        });
    try (var client = client()) {
      client.detectImage(PNG, new EtchvClient.Options("a.png", "scan-0001"));
    }
    assertEquals(List.of("scan-0001", "scan-0001"), keys);
  }

  @Test
  void requestInProgressWaitsForRetryAfterAndSendsAgain() throws Exception {
    var posts = new AtomicInteger();
    serve(
        (e, path, body) -> {
          if (posts.incrementAndGet() == 1) {
            e.getResponseHeaders().set("Retry-After", "1");
            json(e, 409, conflict("request_in_progress"));
          } else json(e, 200, DETECTED);
        });
    long started = System.nanoTime();
    try (var client = client()) {
      assertTrue(client.detectImage(PNG, null).watermarked());
    }
    assertTrue(System.nanoTime() - started >= 900_000_000L);
    assertEquals(2, posts.get());
  }

  @Test
  void replayExpiredAndRequestFailedThrowWithoutRetrying() throws Exception {
    var code = new String[1];
    serve((e, path, body) -> json(e, 409, conflict(code[0])));
    try (var client = client()) {
      for (String value : List.of("replay_expired", "request_failed")) {
        code[0] = value;
        var error =
            assertThrows(EtchvClient.EtchvException.class, () -> client.detectImage(PNG, null));
        assertEquals(409, error.statusCode());
        assertEquals(value, error.code());
        assertNotNull(error.idempotencyKey());
      }
    }
    assertEquals(2, calls.size());
  }

  @Test
  void otherErrorsAreStillNotRetried() throws Exception {
    serve((e, path, body) -> json(e, 503, "{\"detail\":\"busy\"}"));
    try (var client = client()) {
      assertEquals(
          503,
          assertThrows(EtchvClient.EtchvException.class, () -> client.detectImage(PNG, null))
              .statusCode());
    }
    assertEquals(1, calls.size());
  }

  @Test
  void runningOutOfTimeReportsTheKey() throws Exception {
    serve((e, path, body) -> drop(e, DETECTED.getBytes(StandardCharsets.UTF_8), 5));
    try (var client = new EtchvClient("test-key", base, Duration.ofMillis(1500))) {
      var error =
          assertThrows(
              EtchvClient.EtchvException.class,
              () -> client.detectImage(PNG, new EtchvClient.Options("a.png", "scan-0002")));
      assertEquals(0, error.statusCode());
      assertEquals("scan-0002", error.idempotencyKey());
      assertTrue(error.detail().contains("deadline"), error.detail());
    }
  }

  @Test
  void keepResultIsAQueryParameter() throws Exception {
    serve((e, path, body) -> json(e, 200, DETECTED));
    try (var client = client()) {
      client.detectImage(PNG, null);
      client.detectImage(PNG, new EtchvClient.Options().withKeepResult(false));
      client.detectDocument(
          PNG,
          new EtchvClient.Options()
              .withAccelerator(EtchvClient.Accelerator.GPU)
              .withKeepResult(true));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              client.embedImage(
                  PNG, java.util.Map.of("a", 1), new EtchvClient.Options().withKeepResult(false)));
      assertThrows(
          IllegalArgumentException.class,
          () -> client.detectVideo(PNG, new EtchvClient.Options().withKeepResult(true)));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              client.submitDetection(
                  "images", PNG, new EtchvClient.Options().withKeepResult(false), null));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              client.submitEmbed(
                  "images",
                  PNG,
                  java.util.Map.of("a", 1),
                  new EtchvClient.Options().withKeepResult(true),
                  null));
    }
    assertEquals(
        List.of(
            "POST /watermarks/images/detect",
            "POST /watermarks/images/detect?keep_result=false",
            "POST /watermarks/documents/detect?accelerator=gpu&keep_result=true"),
        calls);
  }

  @Test
  void aConsumedUploadIsReplacedByANewUploadWithTheSameKey() throws Exception {
    var uploads = new AtomicInteger();
    var posts = new CopyOnWriteArrayList<String>();
    serve(
        (e, path, body) -> {
          if (path.equals("/uploads")) {
            String upload = "upl_" + String.valueOf(uploads.incrementAndGet()).repeat(32);
            json(
                e,
                201,
                "{\"upload_id\":\""
                    + upload
                    + "\",\"kind\":\"detect\",\"filename\":\"image.png\",\"size\":"
                    + PNG.length
                    + ",\"status\":\"pending\",\"upload\":{\"method\":\"PUT\","
                    + "\"url\":\""
                    + base
                    + "/put/"
                    + upload
                    + "\",\"expires_at\":\"x\"}}");
          } else if (path.startsWith("/put/")) respond(e, 200, null, new byte[0]);
          else {
            String form = new String(body, StandardCharsets.UTF_8);
            String upload = form.split("name=\"upload_id\"\r\n\r\n", 2)[1].substring(0, 36);
            posts.add(e.getRequestHeaders().getFirst("Idempotency-Key") + " " + upload);
            if (posts.size() == 1) drop(e, DETECTED.getBytes(StandardCharsets.UTF_8), 5);
            else if (posts.size() == 2) json(e, 409, conflict("upload_consumed"));
            else json(e, 200, DETECTED);
          }
        });
    try (var client = new EtchvClient("test-key", base, Duration.ofSeconds(10), 100)) {
      client.detectImage(PNG, new EtchvClient.Options().withKeepResult(false));
    }
    String key = posts.get(0).split(" ")[0];
    assertEquals(
        List.of(
            key + " upl_" + "1".repeat(32),
            key + " upl_" + "1".repeat(32),
            key + " upl_" + "2".repeat(32)),
        posts);
  }

  /** Answers {@code POST /uploads} and the upload PUT; the detection itself goes to {@code detect}. */
  private void serveUploads(AtomicInteger uploads, Handler detect) throws IOException {
    serve(
        (e, path, body) -> {
          if (path.equals("/uploads")) session(e, uploads.incrementAndGet());
          else if (path.startsWith("/put/")) respond(e, 200, null, new byte[0]);
          else detect.handle(e, path, body);
        });
  }

  /** A detection upload session {@code upl_nnn…}. */
  private void session(HttpExchange e, int number) throws IOException {
    String upload = "upl_" + String.valueOf(number).repeat(32);
    json(
        e,
        201,
        "{\"upload_id\":\""
            + upload
            + "\",\"kind\":\"detect\",\"filename\":\"image.png\",\"size\":"
            + PNG.length
            + ",\"status\":\"pending\",\"upload\":{\"method\":\"PUT\","
            + "\"url\":\""
            + base
            + "/put/"
            + upload
            + "\",\"expires_at\":\"x\"}}");
  }

  private static String uploadIdOf(byte[] form) {
    return new String(form, StandardCharsets.UTF_8)
        .split("name=\"upload_id\"\r\n\r\n", 2)[1]
        .substring(0, 36);
  }

  @Test
  void aResendWaitsWhileTheFirstCopyStillHoldsTheUpload() throws Exception {
    var uploads = new AtomicInteger();
    var posts = new CopyOnWriteArrayList<String>();
    serveUploads(
        uploads,
        (e, path, body) -> {
          posts.add(e.getRequestHeaders().getFirst("Idempotency-Key") + " " + uploadIdOf(body));
          if (posts.size() == 1) drop(e, DETECTED.getBytes(StandardCharsets.UTF_8), 5);
          else if (posts.size() == 2) json(e, 409, conflict("upload_in_use"));
          else json(e, 200, DETECTED);
        });
    long started = System.nanoTime();
    try (var client = new EtchvClient("test-key", base, Duration.ofSeconds(10), 100)) {
      assertTrue(client.detectImage(PNG, null).watermarked());
    }
    assertTrue(System.nanoTime() - started >= 1_800_000_000L); // 1 s after the drop, 1 s in use.
    assertEquals(1, uploads.get());
    assertEquals(3, posts.size());
    assertEquals(1, posts.stream().distinct().count(), posts.toString());
  }

  @Test
  void uploadInUseOnAFileInTheRequestBodyIsNotWaitedFor() throws Exception {
    var keys = new CopyOnWriteArrayList<String>();
    serve(
        (e, path, body) -> {
          keys.add(e.getRequestHeaders().getFirst("Idempotency-Key"));
          json(e, 409, conflict("upload_in_use"));
        });
    try (var client = client()) {
      var error =
          assertThrows(EtchvClient.EtchvException.class, () -> client.detectImage(PNG, null));
      assertEquals("upload_in_use", error.code());
      assertEquals(keys.get(0), error.idempotencyKey());
    }
    assertEquals(1, keys.size());
  }

  @Test
  void theResendAfterANewUploadKeepsTheFirstDeadline() throws Exception {
    var sessions = new AtomicInteger();
    var puts = new AtomicInteger();
    var posts = new AtomicInteger();
    // The re-upload is slow: its session and PUT together outlast the 2 s timeout, each well
    // within it. A slow runner only adds time, so it can only reach the deadline sooner.
    serve(
        (e, path, body) -> {
          if (path.equals("/uploads")) {
            int number = sessions.incrementAndGet();
            if (number == 2) Thread.sleep(1200);
            session(e, number);
          } else if (path.startsWith("/put/")) {
            if (puts.incrementAndGet() == 2) Thread.sleep(1200);
            respond(e, 200, null, new byte[0]);
          } else if (posts.incrementAndGet() == 1) {
            json(e, 409, conflict("upload_consumed"));
          } else {
            // Reached only with a fresh window per send, the regression this catches.
            json(e, 200, DETECTED);
          }
        });
    try (var client = new EtchvClient("test-key", base, Duration.ofSeconds(2), 100)) {
      var error =
          assertThrows(
              EtchvClient.EtchvException.class,
              () -> client.detectImage(PNG, new EtchvClient.Options("a.png", "scan-0005")));
      assertEquals(0, error.statusCode());
      assertEquals("scan-0005", error.idempotencyKey());
    }
    assertEquals(List.of(1, 2, 2), List.of(posts.get(), sessions.get(), puts.get()));
  }

  @Test
  void aFailedNewUploadCarriesTheKey() throws Exception {
    var uploads = new AtomicInteger();
    serve(
        (e, path, body) -> {
          if (path.equals("/uploads") && uploads.incrementAndGet() > 1)
            json(e, 403, "{\"detail\":\"no uploads\"}");
          else if (path.equals("/uploads")) session(e, 1);
          else if (path.startsWith("/put/")) respond(e, 200, null, new byte[0]);
          else json(e, 409, conflict("upload_consumed"));
        });
    try (var client = new EtchvClient("test-key", base, Duration.ofSeconds(10), 100)) {
      var error =
          assertThrows(
              EtchvClient.EtchvException.class,
              () -> client.detectImage(PNG, new EtchvClient.Options("a.png", "scan-0006")));
      assertEquals(403, error.statusCode());
      assertEquals("scan-0006", error.idempotencyKey());
    }
  }

  @Test
  void downloadsRetryADroppedConnection() throws Exception {
    var gets = new AtomicInteger();
    byte[] file = "file-bytes".getBytes(StandardCharsets.UTF_8);
    serve(
        (e, path, body) -> {
          if (gets.incrementAndGet() % 2 == 1) drop(e, file, 3);
          else respond(e, 200, "application/octet-stream", file);
        });
    try (var client = client()) {
      assertArrayEquals(file, client.downloadAsset(ASSET));
      assertArrayEquals(file, client.downloadStorageDelivery(DELIVERY));
    }
    assertEquals(
        List.of(
            "GET /assets/" + ASSET + "/content",
            "GET /assets/" + ASSET + "/content",
            "GET /storage/deliveries/" + DELIVERY + "/content",
            "GET /storage/deliveries/" + DELIVERY + "/content"),
        calls);
  }

  @Test
  void downloadErrorsAreNotRetriedAndTimeRunsOutOnTheNetworkError() throws Exception {
    var busy = new boolean[] {true};
    serve(
        (e, path, body) -> {
          if (busy[0]) json(e, 503, "{\"detail\":\"busy\"}");
          else drop(e, new byte[10], 3);
        });
    try (var client = new EtchvClient("test-key", base, Duration.ofMillis(1500))) {
      assertEquals(
          503,
          assertThrows(EtchvClient.EtchvException.class, () -> client.downloadAsset(ASSET))
              .statusCode());
      assertEquals(1, calls.size());
      busy[0] = false;
      var error =
          assertThrows(EtchvClient.EtchvException.class, () -> client.downloadAsset(ASSET));
      assertEquals(0, error.statusCode());
      assertInstanceOf(IOException.class, error.getCause());
      assertFalse(error.detail().contains("deadline"), error.detail());
    }
  }

  private static byte[] zip() {
    byte[] zip = new byte[200_000];
    Arrays.fill(zip, (byte) 7);
    zip[0] = 'P';
    zip[1] = 'K';
    return zip;
  }

  @Test
  void aDroppedArchiveStartsAgainFromTheBeginning() throws Exception {
    byte[] zip = zip();
    var gets = new AtomicInteger();
    serve(
        (e, path, body) -> {
          assertNull(e.getRequestHeaders().getFirst("Range"));
          if (gets.incrementAndGet() % 2 == 1) drop(e, zip, 100_000);
          else respond(e, 200, "application/zip", zip);
        });
    var file = temp.resolve("out.zip");
    var handed = temp.resolve("handed.zip");
    Files.write(handed, "header".getBytes(StandardCharsets.UTF_8));
    try (var client = client()) {
      assertEquals(zip.length, client.downloadBatchArchive(BATCH, file, Duration.ofSeconds(30)));
      assertArrayEquals(zip, client.downloadBatchArchive(BATCH, Duration.ofSeconds(30)));
      try (var out = new FileOutputStream(handed.toFile(), true)) {
        assertEquals(zip.length, client.downloadBatchArchive(BATCH, out, Duration.ofSeconds(30)));
      }
    }
    assertArrayEquals(zip, Files.readAllBytes(file));
    var expected = new ByteArrayOutputStream();
    expected.writeBytes("header".getBytes(StandardCharsets.UTF_8));
    expected.writeBytes(zip);
    assertArrayEquals(expected.toByteArray(), Files.readAllBytes(handed));
    try (var listing = Files.list(temp)) {
      assertEquals(2, listing.count()); // No temporary file left behind.
    }
    assertEquals(6, gets.get());
  }

  @Test
  void aDroppedArchiveIntoAnotherStreamThrows() throws Exception {
    byte[] zip = zip();
    serve((e, path, body) -> drop(e, zip, 100_000));
    var out = new ByteArrayOutputStream();
    try (var client = client()) {
      var error =
          assertThrows(
              EtchvClient.EtchvException.class,
              () -> client.downloadBatchArchive(BATCH, out, Duration.ofSeconds(30)));
      assertEquals(0, error.statusCode());
    }
    assertTrue(out.size() > 0 && out.size() <= 100_000);
    assertEquals(1, calls.size());
  }
}
