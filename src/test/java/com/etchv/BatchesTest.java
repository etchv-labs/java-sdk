package com.etchv;

import static org.junit.jupiter.api.Assertions.*;

import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class BatchesTest {
  private static final String BATCH = "bat_" + "a".repeat(32);
  private static final String JOB = "req_" + "b".repeat(64);
  private static final String WATERMARK = "ab".repeat(32);
  private static final byte[] PNG = {(byte) 137, 80, 78, 71, 13, 10, 26, 10, 1, 2, 3};
  private static final byte[] PDF = "%PDF-1.7 test".getBytes(StandardCharsets.US_ASCII);

  private final List<Throwable> failures = new CopyOnWriteArrayList<>();
  private final List<String> calls = new CopyOnWriteArrayList<>();
  private HttpServer server;
  private String base;

  @TempDir java.nio.file.Path temp;

  private interface Handler {
    void handle(HttpExchange exchange, String path, byte[] body) throws Exception;
  }

  private void serve(Handler handler) throws IOException {
    serve(handler, null);
  }

  /** Serves requests; the body of {@code slowPath} is read slowly (256 KB every 20 ms). */
  private void serve(Handler handler, String slowPath) throws IOException {
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
            byte[] body;
            if (uri.getPath().equals(slowPath)) {
              var out = new java.io.ByteArrayOutputStream();
              byte[] chunk;
              while ((chunk = exchange.getRequestBody().readNBytes(256 * 1024)).length > 0) {
                out.writeBytes(chunk);
                Thread.sleep(20);
              }
              body = out.toByteArray();
            } else body = exchange.getRequestBody().readAllBytes();
            handler.handle(exchange, uri.getPath(), body);
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

  private static String batch(String status, String items) {
    return "{\"batch_id\":\""
        + BATCH
        + "\",\"status\":\""
        + status
        + "\",\"item_count\":2,\"archive\":false,\"accelerator\":\"cpu\",\"webhook_id\":null,"
        + "\"storage_destination_id\":null,\"counts\":{\"pending\":2,\"accepted\":0,\"rejected\":0,"
        + "\"succeeded\":0,\"failed\":0,\"in_progress\":0},\"credits\":{\"reserved\":0,\"charged\":0,"
        + "\"refunded\":0},\"cancel_requested\":false,\"created_at\":\"2026-10-09T00:00:00Z\","
        + "\"started_at\":null,\"completed_at\":null,\"upload_expires_at\":\"2026-10-10T00:00:00Z\","
        + "\"status_url\":\"/watermarks/batches/"
        + BATCH
        + "\",\"items\":["
        + items
        + "]}";
  }

  private String pendingItem(int index, String filename, int size) {
    return "{\"index\":"
        + index
        + ",\"filename\":\""
        + filename
        + "\",\"size\":"
        + size
        + ",\"upload_id\":\"upl_"
        + index
        + "\",\"request_id\":null,\"status\":\"pending\",\"error_code\":null,\"error_detail\":null,"
        + "\"credits\":null,\"upload\":{\"method\":\"PUT\",\"url\":\""
        + base
        + "/signed/"
        + index
        + "?Signature=s\",\"expires_at\":\"x\"}}";
  }

  @Test
  void submitBatchCreatesUploadsWithoutTheApiKeyAndStarts() throws Exception {
    var keys = new CopyOnWriteArrayList<String>();
    var uploads = new ConcurrentHashMap<String, byte[]>();
    var pdf = temp.resolve("contract.pdf");
    Files.write(pdf, PDF);
    serve(
        (e, path, body) -> {
          var headers = e.getRequestHeaders();
          if (path.equals("/watermarks/batches")) {
            assertEquals("test-key", headers.getFirst("X-API-Key"));
            keys.add(headers.getFirst("Idempotency-Key"));
            var request = JsonParser.parseString(new String(body, StandardCharsets.UTF_8)).getAsJsonObject();
            var items = request.getAsJsonArray("items");
            assertEquals(2, items.size());
            var first = items.get(0).getAsJsonObject();
            assertEquals("photo.png", first.get("filename").getAsString());
            assertEquals(PNG.length, first.get("size").getAsInt());
            assertEquals("acme", first.getAsJsonObject("data").get("recipient").getAsString());
            assertEquals("contract.pdf", items.get(1).getAsJsonObject().get("filename").getAsString());
            assertEquals(PDF.length, items.get(1).getAsJsonObject().get("size").getAsInt());
            assertFalse(request.get("archive").getAsBoolean());
            json(
                e,
                201,
                batch(
                    "draft",
                    pendingItem(0, "photo.png", PNG.length)
                        + ","
                        + pendingItem(1, "contract.pdf", PDF.length)));
          } else if (path.startsWith("/signed/")) {
            assertNull(headers.getFirst("X-API-Key"));
            assertNull(headers.getFirst("Authorization"));
            assertEquals("PUT", e.getRequestMethod());
            uploads.put(path, body);
            respond(e, 200, null, new byte[0]);
          } else if (path.equals("/watermarks/batches/" + BATCH + "/start")) {
            assertEquals("test-key", headers.getFirst("X-API-Key"));
            json(e, 202, batch("starting", ""));
          } else fail("unexpected " + path);
        });
    try (var client = client()) {
      var started =
          client.submitBatch(
              List.of(
                  EtchvClient.BatchItem.of("photo.png", PNG, Map.of("recipient", "acme")),
                  EtchvClient.BatchItem.of(pdf, Map.of("recipient", "bolt"))),
              null);
      assertEquals("starting", started.status());
      assertEquals(BATCH, started.batchId());
      assertEquals(EtchvClient.Accelerator.CPU, started.accelerator());
      assertFalse(started.isDone());
    }
    assertEquals(1, keys.size());
    assertTrue(keys.get(0).matches("[A-Za-z0-9_-]{8,128}"));
    assertArrayEquals(PNG, uploads.get("/signed/0"));
    assertArrayEquals(PDF, uploads.get("/signed/1"));
    assertEquals("POST /watermarks/batches", calls.get(0));
    assertEquals("POST /watermarks/batches/" + BATCH + "/start", calls.get(calls.size() - 1));
    assertEquals(4, calls.size());
  }

  @Test
  void createRetriesReuseTheIdempotencyKeyAndAReplaySkipsReceivedFiles() throws Exception {
    var keys = new CopyOnWriteArrayList<String>();
    serve(
        (e, path, body) -> {
          if (path.equals("/watermarks/batches")) {
            keys.add(e.getRequestHeaders().getFirst("Idempotency-Key"));
            if (keys.size() == 1) {
              e.getResponseHeaders().set("Retry-After", "0.01");
              json(e, 429, "{\"detail\":{\"code\":\"rate_limited\",\"message\":\"slow down\"}}");
            } else if (keys.size() == 2) json(e, 502, "{\"detail\":\"bad gateway\"}");
            else {
              // Item 0 was already received: only item 1 has an upload URL.
              String received =
                  "{\"index\":0,\"filename\":\"a.png\",\"size\":11,\"status\":\"pending\","
                      + "\"upload_received\":true}";
              json(e, 200, batch("draft", received + "," + pendingItem(1, "b.png", PNG.length)));
            }
          } else if (path.startsWith("/signed/")) respond(e, 200, null, new byte[0]);
          else json(e, 202, batch("starting", ""));
        });
    try (var client = client()) {
      client.submitBatch(
          List.of(
              EtchvClient.BatchItem.of("a.png", PNG, Map.of("n", 1)),
              EtchvClient.BatchItem.of("b.png", PNG, Map.of("n", 2))),
          new EtchvClient.BatchOptions().withIdempotencyKey("batch-key-0001"));
    }
    assertEquals(List.of("batch-key-0001", "batch-key-0001", "batch-key-0001"), keys);
    assertEquals(1, calls.stream().filter(c -> c.startsWith("PUT ")).count());
    assertFalse(calls.contains("PUT /signed/0"));
  }

  @Test
  void unavailableUploadsAreNotRetried() throws Exception {
    serve(
        (e, path, body) ->
            json(
                e,
                503,
                "{\"detail\":\"Batch uploads are unavailable; send one zip to /watermarks/batches/zip instead\"}"));
    try (var client = client()) {
      var error =
          assertThrows(
              EtchvClient.EtchvException.class,
              () ->
                  client.submitBatch(
                      List.of(EtchvClient.BatchItem.of("a.png", PNG, Map.of("n", 1))), null));
      assertEquals(503, error.statusCode());
      assertTrue(error.detail().contains("zip"));
      assertNotNull(error.idempotencyKey());
    }
    assertEquals(List.of("POST /watermarks/batches"), calls);
  }

  @Test
  void aFailedUploadNamesTheItemAndKeepsTheKey() throws Exception {
    serve(
        (e, path, body) -> {
          if (path.equals("/watermarks/batches"))
            json(e, 201, batch("draft", pendingItem(0, "a.png", PNG.length)));
          else respond(e, 403, "text/plain", "Forbidden".getBytes(StandardCharsets.UTF_8));
        });
    try (var client = client()) {
      var error =
          assertThrows(
              EtchvClient.EtchvException.class,
              () ->
                  client.submitBatch(
                      List.of(EtchvClient.BatchItem.of("a.png", PNG, Map.of("n", 1))),
                      new EtchvClient.BatchOptions().withIdempotencyKey("resume-key-01")));
      assertEquals(403, error.statusCode());
      assertEquals("resume-key-01", error.idempotencyKey());
      assertTrue(error.detail().contains("a.png"));
      assertTrue(error.detail().contains(BATCH));
    }
    assertFalse(calls.stream().anyMatch(c -> c.endsWith("/start")));
  }

  @Test
  void moreThanOneHundredFilesFailBeforeAnyRequest() throws Exception {
    var items = new ArrayList<EtchvClient.BatchItem>();
    for (int i = 0; i < 101; i++)
      items.add(EtchvClient.BatchItem.of("f" + i + ".png", PNG, Map.of("i", i)));
    try (var client = new EtchvClient("test-key", "http://127.0.0.1:9", Duration.ofSeconds(1))) {
      var error = assertThrows(IllegalArgumentException.class, () -> client.submitBatch(items, null));
      assertTrue(error.getMessage().contains("100"));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              client.submitBatch(
                  List.of(EtchvClient.BatchItem.of("a.png", PNG, Map.of())), null));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              client.submitBatch(
                  List.of(EtchvClient.BatchItem.of("a.png", PNG, Map.of("n", 1))),
                  new EtchvClient.BatchOptions().withUploadConcurrency(0)));
      assertThrows(IllegalArgumentException.class, () -> client.getBatch("bat_nope"));
    }
  }

  @Test
  void waitForBatchHonorsRetryAfter() throws Exception {
    var polls = new AtomicInteger();
    serve(
        (e, path, body) -> {
          assertEquals("/watermarks/batches/" + BATCH, path);
          if (polls.incrementAndGet() == 1) {
            // Below the 1 s floor: the client still waits a full second.
            e.getResponseHeaders().set("Retry-After", "0.3");
            json(e, 200, batch("processing", ""));
          } else json(e, 200, batch("completed", ""));
        });
    long started = System.nanoTime();
    try (var client = client()) {
      var done = client.waitForBatch(BATCH, Duration.ofSeconds(5));
      assertEquals("completed", done.status());
      assertTrue(done.isDone());
    }
    assertTrue(System.nanoTime() - started >= 1_000_000_000L);
    assertEquals(2, polls.get());
  }

  @Test
  void waitForBatchTimesOut() throws Exception {
    serve(
        (e, path, body) -> {
          e.getResponseHeaders().set("Retry-After", "0.05");
          json(e, 200, batch("processing", ""));
        });
    try (var client = client()) {
      var error =
          assertThrows(
              EtchvClient.EtchvException.class,
              () -> client.waitForBatch(BATCH, Duration.ofMillis(200)));
      assertEquals(0, error.statusCode());
      assertTrue(error.getCause() instanceof TimeoutException);
      assertTrue(error.detail().contains("processing"));
    }
  }

  @Test
  void batchResultsSurfaceEachFile() throws Exception {
    String items =
        "{\"index\":0,\"filename\":\"a.png\",\"size\":11,\"request_id\":\""
            + JOB
            + "\",\"status\":\"succeeded\",\"error_code\":null,\"error_detail\":null,\"credits\":1,"
            + "\"status_url\":\"/watermarks/jobs/"
            + JOB
            + "\",\"result_url\":\"/watermarks/jobs/"
            + JOB
            + "/result\",\"result_expires_at\":\"x\"},"
            + "{\"index\":1,\"filename\":\"b.png\",\"size\":99,\"request_id\":null,\"status\":\"rejected\","
            + "\"error_code\":\"upload_size_mismatch\",\"error_detail\":null,\"credits\":null}";
    serve(
        (e, path, body) -> {
          if (path.equals("/watermarks/batches/" + BATCH)) json(e, 200, batch("completed", items));
          else if (path.equals("/watermarks/jobs/" + JOB + "/result")) {
            e.getResponseHeaders().set("X-Watermark-ID", WATERMARK);
            e.getResponseHeaders().set("X-Request-ID", JOB);
            respond(e, 200, "image/png", PNG);
          } else fail("unexpected " + path);
        });
    try (var client = client()) {
      var results = client.batchResults(BATCH).toList();
      assertEquals(2, results.size());
      var ok = results.get(0);
      assertTrue(ok.ok());
      assertEquals(WATERMARK, ok.result().watermarkId());
      assertArrayEquals(PNG, ok.result().bytes());
      assertEquals(1, ok.credits());
      var rejected = results.get(1);
      assertFalse(rejected.ok());
      assertEquals("rejected", rejected.status());
      assertEquals("upload_size_mismatch", rejected.errorCode());
      assertNull(rejected.error());
    }
  }

  @Test
  void archiveWaitsWhileAssembling() throws Exception {
    byte[] zip = {'P', 'K', 3, 4, 9, 9};
    var gets = new AtomicInteger();
    serve(
        (e, path, body) -> {
          assertEquals("/watermarks/batches/" + BATCH + "/archive", path);
          if (gets.incrementAndGet() == 1) {
            e.getResponseHeaders().set("Retry-After", "0.05");
            json(e, 202, batch("assembling", ""));
          } else respond(e, 200, "application/zip", zip);
        });
    try (var client = client()) {
      assertArrayEquals(zip, client.downloadBatchArchive(BATCH));
    }
    assertEquals(2, gets.get());
  }

  @Test
  void archiveConflictsCarryTheirCode() throws Exception {
    serve(
        (e, path, body) ->
            json(
                e,
                409,
                "{\"detail\":{\"code\":\"archive_not_requested\",\"message\":\"no archive\"}}"));
    try (var client = client()) {
      var error =
          assertThrows(EtchvClient.EtchvException.class, () -> client.downloadBatchArchive(BATCH));
      assertEquals(409, error.statusCode());
      assertEquals("archive_not_requested", error.code());
    }
  }

  @Test
  void submitBatchZipSendsTheZipAndManifest() throws Exception {
    byte[] zip = {'P', 'K', 5, 6, 0, 0};
    serve(
        (e, path, body) -> {
          assertEquals("/watermarks/batches/zip", path);
          assertNotNull(e.getRequestHeaders().getFirst("Idempotency-Key"));
          assertTrue(e.getRequestHeaders().getFirst("Content-Type").startsWith("multipart/form-data"));
          String form = new String(body, StandardCharsets.ISO_8859_1);
          assertTrue(form.contains("name=\"archive\"; filename=\"batch.zip\""));
          assertTrue(form.contains("Content-Type: application/zip\r\n\r\nPK\u0005\u0006\0\0\r\n"));
          assertTrue(form.contains("name=\"manifest\""));
          assertTrue(
              form.contains(
                  "{\"archive\":true,\"items\":[{\"filename\":\"in/a.png\",\"data\":{\"n\":1}}]}"));
          json(e, 202, batch("starting", ""));
        });
    try (var client = client()) {
      var batch =
          client.submitBatchZip(
              zip,
              List.of(new EtchvClient.BatchZipItem("in/a.png", Map.of("n", 1))),
              new EtchvClient.BatchOptions().withArchive(true));
      assertEquals("starting", batch.status());
    }
  }

  @Test
  void archivePollingOutlastsTheClientTimeout() throws Exception {
    byte[] zip = {'P', 'K', 3, 4, 1};
    var gets = new AtomicInteger();
    serve(
        (e, path, body) -> {
          if (gets.incrementAndGet() == 1) {
            e.getResponseHeaders().set("Retry-After", "1");
            json(e, 202, batch("assembling", ""));
          } else respond(e, 200, "application/zip", zip);
        });
    long started = System.nanoTime();
    try (var client = new EtchvClient("test-key", base, Duration.ofMillis(300))) {
      assertArrayEquals(zip, client.downloadBatchArchive(BATCH, Duration.ofSeconds(5)));
    }
    assertTrue(System.nanoTime() - started >= 1_000_000_000L);
    assertEquals(2, gets.get());
  }

  @Test
  void archiveNotReadyByTheWaitTimeout() throws Exception {
    serve(
        (e, path, body) -> {
          e.getResponseHeaders().set("Retry-After", "0.05");
          json(e, 202, batch("processing", ""));
        });
    try (var client = client()) {
      var error =
          assertThrows(
              EtchvClient.EtchvException.class,
              () -> client.downloadBatchArchive(BATCH, Duration.ofMillis(300)));
      assertEquals(0, error.statusCode());
      assertTrue(error.detail().contains("not ready"));
      assertTrue(error.getCause() instanceof TimeoutException);
    }
  }

  @Test
  void archiveStreamsToAStreamOrAFile() throws Exception {
    byte[] zip = new byte[200_000];
    zip[0] = 'P';
    zip[1] = 'K';
    zip[zip.length - 1] = 7;
    serve((e, path, body) -> respond(e, 200, "application/zip", zip));
    try (var client = client()) {
      var out = new java.io.ByteArrayOutputStream();
      assertEquals(zip.length, client.downloadBatchArchive(BATCH, out, null));
      assertArrayEquals(zip, out.toByteArray());
      var file = temp.resolve("out.zip");
      assertEquals(zip.length, client.downloadBatchArchive(BATCH, file, Duration.ofSeconds(5)));
      assertArrayEquals(zip, Files.readAllBytes(file));
      try (var listing = Files.list(temp)) {
        assertEquals(1, listing.count()); // No temporary file left behind.
      }
    }
  }

  @Test
  void archiveOverTheCapOrNotAZipIsRefusedAtOnce() throws Exception {
    assertEquals(1024L * 1024 * 1024 + 64L * 1024 * 1024, EtchvClient.MAX_BATCH_ARCHIVE_SIZE);
    var gets = new AtomicInteger();
    serve(
        (e, path, body) -> {
          if (gets.incrementAndGet() == 1) {
            e.getResponseHeaders().set("Content-Type", "application/zip");
            e.sendResponseHeaders(200, EtchvClient.MAX_BATCH_ARCHIVE_SIZE + 1);
            e.getResponseBody().write(new byte[] {'P', 'K'});
            e.getResponseBody().flush();
          } else respond(e, 200, "application/zip", "not a zip".getBytes(StandardCharsets.UTF_8));
        });
    try (var client = client()) {
      var large =
          assertThrows(EtchvClient.EtchvException.class, () -> client.downloadBatchArchive(BATCH));
      assertTrue(large.detail().contains("exceeds"), large.detail());
      assertEquals("archive_too_large", large.code());
      var invalid =
          assertThrows(EtchvClient.EtchvException.class, () -> client.downloadBatchArchive(BATCH));
      assertEquals("Invalid archive response", invalid.detail());
    }
    assertEquals(2, gets.get());
  }

  @Test
  void aStalledArchiveDownloadFails() throws Exception {
    serve(
        (e, path, body) -> {
          e.getResponseHeaders().set("Content-Type", "application/zip");
          e.sendResponseHeaders(200, 0);
          e.getResponseBody().write(new byte[] {'P', 'K', 1});
          e.getResponseBody().flush();
          Thread.sleep(1500);
        });
    try (var client = new EtchvClient("test-key", base, Duration.ofMillis(300))) {
      var error =
          assertThrows(
              EtchvClient.EtchvException.class,
              () -> client.downloadBatchArchive(BATCH, Duration.ofSeconds(5)));
      assertTrue(error.detail().contains("stalled"), error.detail());
      assertTrue(error.getCause() instanceof TimeoutException);
    }
  }

  @Test
  void aSlowUploadKeepsGoingWhileBytesFlow() throws Exception {
    byte[] big = new byte[12 * 1024 * 1024];
    var received = new AtomicInteger();
    serve(
        (e, path, body) -> {
          if (path.equals("/uploads"))
            json(
                e,
                201,
                "{\"upload_id\":\"upl_1\",\"kind\":\"image\",\"filename\":\"big.png\",\"size\":"
                    + big.length
                    + ",\"status\":\"pending\",\"upload\":{\"method\":\"PUT\",\"url\":\""
                    + base
                    + "/slow\"}}");
          else {
            received.set(body.length);
            respond(e, 200, null, new byte[0]);
          }
        },
        "/slow");
    long started = System.nanoTime();
    try (var client = new EtchvClient("test-key", base, Duration.ofMillis(700))) {
      assertEquals("received", client.uploadFile("image", big, "big.png").status());
    }
    assertEquals(big.length, received.get());
    assertTrue(System.nanoTime() - started > 700_000_000L, "the upload was not slow");
    assertEquals(2, calls.size());
  }

  @Test
  void batchResultsWaitAndExplainCancelledFiles() throws Exception {
    var polls = new AtomicInteger();
    String pending =
        "{\"index\":0,\"filename\":\"a.png\",\"size\":11,\"request_id\":null,\"status\":\"pending\","
            + "\"error_code\":null,\"error_detail\":null,\"credits\":null}";
    serve(
        (e, path, body) -> {
          if (polls.incrementAndGet() == 1) {
            e.getResponseHeaders().set("Retry-After", "0.05");
            json(e, 200, batch("processing", pending));
          } else json(e, 200, batch("cancelled", pending));
        });
    try (var client = client()) {
      var results = client.batchResults(BATCH, Duration.ofSeconds(5)).toList();
      assertEquals(1, results.size());
      assertFalse(results.get(0).ok());
      assertEquals("cancelled", results.get(0).errorCode());
      assertEquals("pending", results.get(0).status());
    }
    assertEquals(2, polls.get());
  }

  @Test
  void anExpiredReplayFailsWithoutUploading() throws Exception {
    serve((e, path, body) -> json(e, 200, batch("expired", pendingItem(0, "a.png", PNG.length))));
    try (var client = client()) {
      var error =
          assertThrows(
              EtchvClient.EtchvException.class,
              () ->
                  client.submitBatch(
                      List.of(EtchvClient.BatchItem.of("a.png", PNG, Map.of("n", 1))),
                      new EtchvClient.BatchOptions().withIdempotencyKey("old-key-0001")));
      assertEquals(410, error.statusCode());
      assertEquals("batch_expired", error.code());
      assertTrue(error.detailMessage().contains(BATCH));
      assertTrue(error.getMessage().contains("expired"));
      assertEquals("old-key-0001", error.idempotencyKey());
    }
    assertEquals(List.of("POST /watermarks/batches"), calls);
  }

  @Test
  void aDraftItemWithoutAnUploadLinkFailsBeforeAnyUpload() throws Exception {
    // upload_received null reads as not received.
    String lost =
        "{\"index\":1,\"filename\":\"b.png\",\"size\":11,\"status\":\"pending\","
            + "\"upload_received\":null}";
    serve(
        (e, path, body) ->
            json(e, 201, batch("draft", pendingItem(0, "a.png", PNG.length) + "," + lost)));
    try (var client = client()) {
      var error =
          assertThrows(
              EtchvClient.EtchvException.class,
              () ->
                  client.submitBatch(
                      List.of(
                          EtchvClient.BatchItem.of("a.png", PNG, Map.of("n", 1)),
                          EtchvClient.BatchItem.of("b.png", PNG, Map.of("n", 2))),
                      null));
      assertTrue(error.detail().contains("item 1 (b.png)"), error.detail());
      assertNotNull(error.idempotencyKey());
    }
    assertEquals(List.of("POST /watermarks/batches"), calls);
  }

  @Test
  void timeInTheCallersStreamIsNotIdleTime() throws Exception {
    byte[] zip = new byte[200_000];
    zip[0] = 'P';
    zip[1] = 'K';
    serve((e, path, body) -> respond(e, 200, "application/zip", zip));
    var slow =
        new java.io.ByteArrayOutputStream() {
          @Override
          public synchronized void write(byte[] bytes, int offset, int length) {
            try {
              Thread.sleep(400); // Longer than the 300 ms client timeout.
            } catch (InterruptedException interrupted) {
              Thread.currentThread().interrupt();
            }
            super.write(bytes, offset, length);
          }
        };
    try (var client = new EtchvClient("test-key", base, Duration.ofMillis(300))) {
      assertEquals(zip.length, client.downloadBatchArchive(BATCH, slow, Duration.ofSeconds(5)));
    }
    assertArrayEquals(zip, slow.toByteArray());
  }

  @Test
  void aStartedReplayIsReturnedAsIs() throws Exception {
    serve((e, path, body) -> json(e, 200, batch("processing", "")));
    try (var client = client()) {
      var batch =
          client.submitBatch(List.of(EtchvClient.BatchItem.of("a.png", PNG, Map.of("n", 1))), null);
      assertEquals("processing", batch.status());
    }
    assertEquals(List.of("POST /watermarks/batches"), calls);
  }

  @Test
  void anUploadForAnUnknownItemIsAnInvalidResponse() throws Exception {
    serve((e, path, body) -> json(e, 201, batch("draft", pendingItem(5, "x.png", PNG.length))));
    try (var client = client()) {
      var error =
          assertThrows(
              EtchvClient.EtchvException.class,
              () ->
                  client.submitBatch(
                      List.of(EtchvClient.BatchItem.of("a.png", PNG, Map.of("n", 1))), null));
      assertEquals("Invalid batch upload response", error.detail());
    }
    assertEquals(List.of("POST /watermarks/batches"), calls);
  }

  @Test
  void aFileThatChangedSizeFailsAtOnce() throws Exception {
    var file = temp.resolve("a.png");
    Files.write(file, PNG);
    serve(
        (e, path, body) -> {
          Files.write(file, new byte[] {1, 2, 3}, java.nio.file.StandardOpenOption.APPEND);
          json(e, 201, batch("draft", pendingItem(0, "a.png", PNG.length)));
        });
    try (var client = client()) {
      var error =
          assertThrows(
              EtchvClient.EtchvException.class,
              () -> client.submitBatch(List.of(EtchvClient.BatchItem.of(file, Map.of("n", 1))), null));
      assertTrue(error.detail().contains("changed size"), error.detail());
      assertNotNull(error.idempotencyKey());
    }
    assertEquals(List.of("POST /watermarks/batches"), calls);
  }

  @Test
  void cancelAndList() throws Exception {
    String before = "bat_" + "c".repeat(32);
    serve(
        (e, path, body) -> {
          if (path.endsWith("/cancel")) json(e, 200, batch("cancelled", ""));
          else
            json(
                e,
                200,
                "{\"data\":[" + batch("completed", "") + "],\"next_cursor\":\"" + before + "\"}");
        });
    try (var client = client()) {
      assertEquals("cancelled", client.cancelBatch(BATCH).status());
      var page = client.listBatches(10, before);
      assertEquals(1, page.items().size());
      assertEquals(before, page.nextCursor());
      assertThrows(IllegalArgumentException.class, () -> client.listBatches(51, null));
    }
    assertEquals(
        List.of(
            "POST /watermarks/batches/" + BATCH + "/cancel",
            "GET /watermarks/batches?limit=10&before=" + before),
        calls);
  }
}
