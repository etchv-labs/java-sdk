package com.etchv;

import static org.junit.jupiter.api.Assertions.*;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

final class LinksTest {
  private static final String ASSET = "ast_" + "e".repeat(64);
  private static final String LINK = "lnk_" + "a".repeat(32);
  private static final String EVENT = "lev_" + "b".repeat(32);
  private static final String HOOK = "wh_" + "c".repeat(32);
  private static final String JOB = "req_" + "d".repeat(64);
  private static final String WATERMARK = "ab".repeat(32);
  private static final byte[] PNG = {(byte) 137, 80, 78, 71, 13, 10, 26, 10, 1, 2, 3};

  private final List<Throwable> failures = new CopyOnWriteArrayList<>();
  private final List<String> calls = new CopyOnWriteArrayList<>();
  private final List<String> keys = new CopyOnWriteArrayList<>();
  private final List<JsonObject> bodies = new CopyOnWriteArrayList<>();
  private HttpServer server;
  private String base;

  private record Reply(int status, String body) {}

  /** Serves JSON: {@code handler} maps "METHOD /path?query" to a reply. */
  private void serve(Function<String, Reply> handler) throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    base = "http://127.0.0.1:" + server.getAddress().getPort();
    server.createContext(
        "/",
        exchange -> {
          try {
            assertEquals("test-key", exchange.getRequestHeaders().getFirst("X-API-Key"));
            var uri = exchange.getRequestURI();
            String route =
                exchange.getRequestMethod()
                    + " "
                    + uri.getPath()
                    + (uri.getQuery() == null ? "" : "?" + uri.getQuery());
            calls.add(route);
            byte[] body = exchange.getRequestBody().readAllBytes();
            String key = exchange.getRequestHeaders().getFirst("Idempotency-Key");
            if (key != null) keys.add(key);
            if (body.length > 0
                && "application/json".equals(exchange.getRequestHeaders().getFirst("Content-Type")))
              bodies.add(JsonParser.parseString(new String(body, StandardCharsets.UTF_8)).getAsJsonObject());
            var reply = handler.apply(route);
            reply(exchange, reply.status(), reply.body());
          } catch (Throwable error) {
            failures.add(error);
            exchange.sendResponseHeaders(500, -1);
          } finally {
            exchange.close();
          }
        });
    server.start();
  }

  private static void reply(HttpExchange e, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    e.getResponseHeaders().set("Content-Type", "application/json");
    e.getResponseHeaders().set("X-Request-ID", JOB);
    e.sendResponseHeaders(status, bytes.length);
    e.getResponseBody().write(bytes);
  }

  @AfterEach
  void stop() {
    if (server != null) server.stop(0);
    assertTrue(failures.isEmpty(), failures.toString());
  }

  private EtchvClient client() {
    return new EtchvClient("test-key", base, Duration.ofSeconds(10));
  }

  private static String link(String status) {
    return "{\"id\":\""
        + LINK
        + "\",\"url\":\"https://etchv.com/l/Yp3kQ8vN2rT6wX1zA4bC7d\",\"name\":\"Board pack\","
        + "\"asset_id\":\""
        + ASSET
        + "\",\"media_type\":\"document\",\"status\":\""
        + status
        + "\",\"prepare\":\"on_open\",\"allowed_domains\":[],\"expires_at\":\"2026-11-26T14:02:00Z\","
        + "\"download_limit\":3,\"recipient_count\":1,\"credits_per_copy\":2,\"estimated_credits\":2,"
        + "\"batch_id\":null,\"webhook_id\":null,\"created_at\":\"2026-10-28T00:00:00Z\",\"revoked_at\":null,"
        + "\"recipients\":[{\"email\":\"alex@example.com\",\"status\":\"opened\","
        + "\"opened_at\":\"2026-10-28T01:00:00Z\",\"last_opened_at\":\"2026-10-28T02:00:00Z\","
        + "\"open_count\":2,\"download_count\":1,\"copy\":{\"status\":\"ready\",\"asset_id\":\""
        + ASSET
        + "\",\"error_code\":null}}]}";
  }

  @Test
  void createSendsTheBodyAndKeepsOneKeyAcrossRetries() throws Exception {
    serve(
        route -> {
          assertEquals("POST /links", route);
          return calls.size() == 1
              ? new Reply(503, "{\"detail\":\"busy\"}")
              : new Reply(201, link("active"));
        });
    try (var client = client()) {
      var created =
          client.createLink(
              ASSET,
              "Board pack",
              EtchvClient.LinkOptions.recipients(List.of("alex@example.com"))
                  .withExpiresAt(Instant.parse("2026-11-26T14:02:00Z"))
                  .withPrepare(EtchvClient.LinkPrepare.ON_OPEN)
                  .withDownloadLimit(3)
                  .withWebhookId(HOOK));
      assertEquals(LINK, created.id());
      assertEquals(3, created.downloadLimit());
      assertEquals(2, created.estimatedCredits());
      assertNull(created.batchId());
      var recipient = created.recipients().get(0);
      assertEquals("alex@example.com", recipient.email());
      assertEquals(2, recipient.openCount());
      assertEquals("ready", recipient.copy().status());
      assertEquals(ASSET, recipient.copy().assetId());
    }
    assertEquals(2, keys.size());
    assertFalse(keys.get(0).isBlank());
    assertEquals(keys.get(0), keys.get(1));
    var expected =
        JsonParser.parseString(
                "{\"asset_id\":\""
                    + ASSET
                    + "\",\"name\":\"Board pack\",\"recipients\":[\"alex@example.com\"],"
                    + "\"expires_at\":\"2026-11-26T14:02:00Z\",\"prepare\":\"on_open\","
                    + "\"download_limit\":3,\"webhook_id\":\""
                    + HOOK
                    + "\"}")
            .getAsJsonObject();
    assertEquals(expected, bodies.get(0));
    assertEquals(expected, bodies.get(1));
  }

  @Test
  void createForDomainsWithAGivenKey() throws Exception {
    serve(route -> new Reply(200, "{\"id\":\"" + LINK + "\",\"allowed_domains\":[\"example.com\"]}"));
    try (var client = client()) {
      var created =
          client.createLink(
              ASSET,
              "Board pack",
              EtchvClient.LinkOptions.allowedDomains(List.of("example.com"))
                  .withIdempotencyKey("board-pack-2026-10"));
      assertEquals(List.of("example.com"), created.allowedDomains());
      assertEquals(List.of(), created.recipients());
    }
    assertEquals(List.of("board-pack-2026-10"), keys);
    assertEquals(
        JsonParser.parseString(
            "{\"asset_id\":\"" + ASSET + "\",\"name\":\"Board pack\",\"allowed_domains\":[\"example.com\"]}"),
        bodies.get(0));
  }

  @Test
  void tooFewCreditsIsA402WithItsCode() throws Exception {
    serve(
        route ->
            new Reply(
                402,
                "{\"detail\":{\"code\":\"insufficient_credits\",\"message\":\"This link needs 8"
                    + " credits for its copies; 2 are available\",\"estimated_credits\":8,"
                    + "\"available_credits\":2}}"));
    try (var client = client()) {
      var e =
          assertThrows(
              EtchvClient.EtchvException.class,
              () ->
                  client.createLink(
                      ASSET, "Board pack", EtchvClient.LinkOptions.recipients(List.of("a@b.co"))));
      assertEquals(402, e.statusCode());
      assertEquals("insufficient_credits", e.code());
      assertNotNull(e.idempotencyKey());
    }
    assertEquals(1, calls.size());
  }

  @Test
  void invalidInputIsRefusedBeforeAnyRequest() throws Exception {
    serve(route -> new Reply(500, "{}"));
    var recipients = EtchvClient.LinkOptions.recipients(List.of("a@b.co"));
    try (var client = client()) {
      List<Executable> cases =
          List.of(
              () -> client.createLink(ASSET, "Board pack", null),
              () -> client.createLink(ASSET, "Board pack", EtchvClient.LinkOptions.recipients(List.of())),
              () ->
                  client.createLink(
                      ASSET,
                      "Board pack",
                      new EtchvClient.LinkOptions(
                          List.of("a@b.co"), List.of("b.co"), null, null, null, null, null)),
              () -> client.createLink("ast_1", "Board pack", recipients),
              () -> client.createLink(ASSET, " ", recipients),
              () -> client.createLink(ASSET, null, recipients),
              () -> client.createLink(ASSET, "📄".repeat(201), recipients),
              () -> client.createLink(ASSET, "Board pack", recipients.withDownloadLimit(0)),
              () -> client.createLink(ASSET, "Board pack", recipients.withDownloadLimit(101)),
              () -> client.createLink(ASSET, "Board pack", recipients.withWebhookId("wh_1")),
              () -> client.createLink(ASSET, "Board pack", recipients.withIdempotencyKey("short")),
              () -> client.createLink(ASSET, "Board pack", recipients.withIdempotencyKey("bad key!")),
              () -> client.estimateLink("ast_1", "Board pack", recipients),
              () -> client.getLink("lnk_1"),
              () -> client.revokeLink(null),
              () -> client.listLinks(0, null),
              () -> client.listLinks(51, null),
              () -> client.listLinks(null, EVENT),
              () -> client.listLinkEvents("lnk_1", null, null),
              () -> client.listLinkEvents(LINK, 101, null),
              () -> client.listLinkEvents(LINK, null, LINK));
      for (var call : cases) assertThrows(IllegalArgumentException.class, call);
    }
    assertEquals(List.of(), calls);
  }

  @Test
  void twoHundredCharactersAreCountedAsCodePoints() throws Exception {
    serve(route -> new Reply(200, "{\"id\":\"" + LINK + "\"}"));
    String name = "📄".repeat(200); // 400 UTF-16 units, 200 characters
    try (var client = client()) {
      client.createLink(ASSET, name, EtchvClient.LinkOptions.recipients(List.of("a@b.co")));
    }
    assertEquals(name, bodies.get(0).get("name").getAsString());
  }

  @Test
  void routesAndQueries() throws Exception {
    serve(
        route -> {
          if (route.equals("POST /links/estimate"))
            return new Reply(
                200,
                "{\"asset_id\":\""
                    + ASSET
                    + "\",\"media_type\":\"document\",\"prepare\":\"on_create\",\"credits_per_copy\":2,"
                    + "\"estimated_credits\":null,\"available_credits\":10,\"enough_credits\":true,"
                    + "\"latest_expires_at\":\"2027-10-28T00:00:00Z\"}");
          if (route.equals("GET /links?limit=5&before=" + LINK))
            return new Reply(
                200,
                "{\"data\":[{\"id\":\"" + LINK + "\",\"status\":\"active\"}],\"next_cursor\":\"" + LINK + "\"}");
          if (route.equals("GET /links"))
            return new Reply(200, "{\"data\":[],\"next_cursor\":null}");
          if (route.equals("GET /links/" + LINK)) return new Reply(200, link("active"));
          if (route.equals("GET /links/" + LINK + "/events?before=" + EVENT))
            return new Reply(
                200,
                "{\"data\":[{\"id\":\""
                    + EVENT
                    + "\",\"type\":\"downloaded\",\"recipient\":\"alex@example.com\","
                    + "\"created_at\":\"2026-10-28T00:00:00Z\",\"verified_by\":null,\"asset_id\":\""
                    + ASSET
                    + "\"}],\"next_cursor\":null}");
          if (route.equals("GET /links/" + LINK + "/events?limit=100"))
            return new Reply(200, "{\"data\":[],\"next_cursor\":null}");
          if (route.equals("POST /links/" + LINK + "/revoke")) return new Reply(200, link("revoked"));
          throw new AssertionError("unexpected " + route);
        });
    try (var client = client()) {
      var estimate =
          client.estimateLink(
              ASSET,
              "Board pack",
              EtchvClient.LinkOptions.allowedDomains(List.of("example.com"))
                  .withIdempotencyKey("ignored-for-estimates"));
      assertTrue(estimate.enoughCredits());
      assertNull(estimate.estimatedCredits());
      assertEquals(10, estimate.availableCredits());
      assertEquals("on_create", estimate.prepare());

      var page = client.listLinks(5, LINK);
      assertEquals(LINK, page.nextCursor());
      assertEquals(LINK, page.items().get(0).id());
      assertEquals(List.of(), page.items().get(0).recipients());
      assertNull(client.listLinks(null, null).nextCursor());

      var read = client.getLink(LINK);
      assertEquals("opened", read.recipients().get(0).status());

      var events = client.listLinkEvents(LINK, null, EVENT);
      assertNull(events.nextCursor());
      assertEquals("downloaded", events.items().get(0).type());
      assertEquals(ASSET, events.items().get(0).assetId());
      assertEquals(List.of(), client.listLinkEvents(LINK, 100, null).items());

      assertEquals("revoked", client.revokeLink(LINK).status());
    }
    // The estimate is a single request and sends no idempotency key.
    assertEquals(List.of(), keys);
    assertEquals(7, calls.size());
  }

  @Test
  void detectionNamesTheCopyOrAssetWhenPresent() throws Exception {
    String top = "\"watermarked\":true,\"confidence\":0.99,\"watermark_id\":\"" + WATERMARK + "\"";
    String copy =
        "{"
            + top
            + ",\"copy\":{\"asset_id\":\""
            + ASSET
            + "\",\"link\":{\"id\":\""
            + LINK
            + "\",\"name\":\"Board pack\"},\"recipient\":\"alex@example.com\",\"opened_at\":null}}";
    String asset =
        "{"
            + top
            + ",\"copy\":null,\"asset\":{\"asset_id\":\""
            + ASSET
            + "\",\"name\":\"a.png\",\"metadata\":{\"recipient\":\"acme\"},"
            + "\"created_at\":\"2026-10-27T00:00:00Z\"}}";
    String neither = "{" + top + "}";
    serve(
        route -> {
          if (route.equals("POST /watermarks/images/detect"))
            return new Reply(200, calls.size() == 1 ? copy : neither);
          if (route.equals("GET /watermarks/detection-jobs/" + JOB + "/result"))
            return new Reply(200, asset);
          throw new AssertionError("unexpected " + route);
        });
    try (var client = client()) {
      var found = client.detectImage(PNG, new EtchvClient.Options("a.png", null));
      assertNotNull(found.copy());
      assertEquals(ASSET, found.copy().assetId());
      assertEquals(LINK, found.copy().link().id());
      assertEquals("Board pack", found.copy().link().name());
      assertEquals("alex@example.com", found.copy().recipient());
      assertNull(found.copy().openedAt());
      assertNull(found.asset());

      var plain = client.detectImage(PNG, new EtchvClient.Options("a.png", null));
      assertNull(plain.copy());
      assertNull(plain.asset());

      var async = client.getDetectionResult(JOB);
      assertNull(async.copy());
      assertEquals("a.png", async.asset().name());
      assertEquals("acme", async.asset().metadata().get("recipient").getAsString());
      assertEquals("2026-10-27T00:00:00Z", async.asset().createdAt());
    }
  }

  @Test
  void detectionResultKeepsItsEarlierConstructors() {
    var units = List.of(new EtchvClient.DetectionUnit(0, false, 0.1, null));
    var five = new EtchvClient.DetectionResult(false, 0.1, null, JOB, units);
    var six = new EtchvClient.DetectionResult(false, 0.1, null, JOB, units, null);
    assertEquals(five, six);
    assertNull(six.copy());
    assertNull(six.asset());
  }
}
