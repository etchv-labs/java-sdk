package com.etchv;

import static org.junit.jupiter.api.Assertions.*;

import com.google.gson.*;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

final class ManagementTest {
  static final String WEBHOOK = "wh_" + "a".repeat(32),
      EVENT = "evt_" + "b".repeat(64),
      DESTINATION = "dst_" + "c".repeat(32),
      DELIVERY = "std_" + "d".repeat(64),
      ASSET = "ast_" + "e".repeat(64),
      JOB = "req_" + "f".repeat(64),
      SECRET = "whsec_" + "9".repeat(64);

  /** Route key: "METHOD /path?query" -> {status, body}. */
  record Reply(int status, String body) {}

  @Test
  void managementEndpoints() throws Exception {
    var failures = new CopyOnWriteArrayList<Throwable>();
    var seen = new CopyOnWriteArrayList<String>();
    var bodies = new HashMap<String, JsonObject>();
    var routes = new HashMap<String, Reply>();
    String endpoint =
        "{\"id\":\"" + WEBHOOK + "\",\"url\":\"https://example.com/hook\",\"enabled\":true,"
            + "\"created_at\":\"2026-09-12T00:00:00Z\"}";
    String destination =
        "{\"id\":\"" + DESTINATION + "\",\"name\":\"exports\",\"provider\":\"s3\",\"enabled\":true}";
    String delivery = "{\"id\":\"" + DELIVERY + "\",\"status\":\"queued\",\"asset_id\":\"" + ASSET + "\"}";
    routes.put(
        "GET /auth/api-key",
        new Reply(
            200,
            "{\"organization_id\":\"org_1\",\"key_id\":\"key_1\",\"scopes\":[\"webhooks:read\",\"storage:read\"]}"));
    routes.put("GET /webhooks", new Reply(200, "[" + endpoint + "]"));
    routes.put(
        "POST /webhooks",
        new Reply(201, endpoint.replace("}", ",\"signing_secret\":\"" + SECRET + "\"}")));
    routes.put("PATCH /webhooks/" + WEBHOOK, new Reply(200, endpoint.replace("true", "false")));
    routes.put("DELETE /webhooks/" + WEBHOOK, new Reply(204, ""));
    routes.put(
        "GET /webhooks/" + WEBHOOK + "/deliveries?after=" + EVENT,
        new Reply(
            200,
            "{\"data\":[{\"id\":\"" + EVENT + "\",\"request_id\":\"" + JOB + "\",\"status\":\"delivered\","
                + "\"attempts\":1,\"created_at\":\"t\",\"next_attempt_at\":\"t\",\"history\":[],"
                + "\"payload\":{\"id\":\"" + EVENT + "\"}}],\"next_cursor\":null}"));
    routes.put(
        "POST /webhooks/" + WEBHOOK + "/deliveries/" + EVENT + "/redeliver",
        new Reply(202, "{\"id\":\"" + EVENT + "\",\"status\":\"queued\"}"));
    routes.put("GET /storage/destinations", new Reply(200, "[" + destination + "]"));
    routes.put("POST /storage/destinations", new Reply(201, destination));
    routes.put("PATCH /storage/destinations/" + DESTINATION, new Reply(200, destination));
    routes.put("DELETE /storage/destinations/" + DESTINATION, new Reply(204, ""));
    routes.put("POST /storage/destinations/" + DESTINATION + "/verify", new Reply(200, destination));
    routes.put(
        "GET /storage/destinations/" + DESTINATION + "/deliveries",
        new Reply(200, "{\"items\":[" + delivery + "],\"next_cursor\":null}"));
    routes.put("POST /storage/destinations/" + DESTINATION + "/deliveries", new Reply(202, delivery));
    routes.put("GET /storage/deliveries/" + DELIVERY, new Reply(200, delivery));
    routes.put("POST /storage/deliveries/" + DELIVERY + "/retry", new Reply(202, delivery));
    routes.put("GET /storage/deliveries/" + DELIVERY + "/content", new Reply(200, "stored-bytes"));
    routes.put(
        "GET /assets/" + ASSET + "?include_metadata=false",
        new Reply(200, Files.readString(Path.of("tests/assets.json")).replace(ASSET_FIXTURE_ID, ASSET)));
    routes.put(
        "GET /watermarks/jobs/" + JOB + "/result",
        new Reply(410, "{\"status\":\"expired\",\"request_id\":\"" + JOB + "\"}"));
    routes.put(
        "GET /watermarks/detection-jobs/" + JOB,
        new Reply(200, "{\"request_id\":\"" + JOB + "\",\"status\":\"running\"}"));

    var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        e -> {
          int status = 500;
          byte[] out = new byte[0];
          try {
            assertEquals("test-key", e.getRequestHeaders().getFirst("X-API-Key"));
            assertEquals(
                "etchv-java/" + EtchvClient.VERSION, e.getRequestHeaders().getFirst("User-Agent"));
            String query = e.getRequestURI().getRawQuery();
            String route =
                e.getRequestMethod() + " " + e.getRequestURI().getPath() + (query == null ? "" : "?" + query);
            seen.add(route);
            byte[] in = e.getRequestBody().readAllBytes();
            if (in.length > 0) {
              assertEquals("application/json", e.getRequestHeaders().getFirst("Content-Type"));
              bodies.put(route, JsonParser.parseString(new String(in, StandardCharsets.UTF_8)).getAsJsonObject());
            }
            var reply = routes.get(route);
            assertNotNull(reply, "Unexpected route " + route);
            status = reply.status();
            out = reply.body().getBytes(StandardCharsets.UTF_8);
            e.getResponseHeaders().set("X-Request-ID", JOB);
          } catch (Throwable error) {
            failures.add(error);
          }
          e.sendResponseHeaders(status, status == 204 ? -1 : out.length);
          if (status != 204) e.getResponseBody().write(out);
          e.close();
        });
    server.start();
    try (var c =
        new EtchvClient(
            "test-key", "http://127.0.0.1:" + server.getAddress().getPort(), Duration.ofSeconds(2))) {
      assertFalse(c.toString().contains("test-key"));

      var identity = c.getApiKeyInfo();
      assertEquals("org_1", identity.organizationId());
      assertEquals("key_1", identity.keyId());
      assertEquals(List.of("webhooks:read", "storage:read"), identity.scopes());

      assertEquals(WEBHOOK, c.listWebhooks().get(0).id());
      var created = c.createWebhook("https://example.com/hook");
      assertEquals(SECRET, created.signingSecret());
      assertFalse(created.toString().contains(SECRET));
      assertEquals("https://example.com/hook", bodies.get("POST /webhooks").get("url").getAsString());
      assertFalse(c.updateWebhook(WEBHOOK, false).enabled());
      assertFalse(bodies.get("PATCH /webhooks/" + WEBHOOK).get("enabled").getAsBoolean());
      var page = c.listWebhookDeliveries(WEBHOOK, EVENT);
      assertEquals(1, page.items().size());
      assertEquals(JOB, page.items().get(0).requestId());
      assertEquals(EVENT, page.items().get(0).payload().get("id").getAsString());
      assertNull(page.nextCursor());
      assertEquals("queued", c.redeliverWebhook(WEBHOOK, EVENT).get("status").getAsString());
      c.deleteWebhook(WEBHOOK);

      assertEquals("exports", c.listStorageDestinations().get(0).get("name").getAsString());
      c.createStorageDestination(Map.of("name", "exports", "provider", "s3"));
      c.updateStorageDestination(DESTINATION, Map.of("enabled", false));
      assertEquals(DESTINATION, c.verifyStorageDestination(DESTINATION).get("id").getAsString());
      assertEquals(1, c.listStorageDeliveries(DESTINATION, null).getAsJsonArray("items").size());
      assertEquals("queued", c.createStorageDelivery(DESTINATION, ASSET, "a/b.png").get("status").getAsString());
      var deliveryBody = bodies.get("POST /storage/destinations/" + DESTINATION + "/deliveries");
      assertEquals(ASSET, deliveryBody.get("asset_id").getAsString());
      assertEquals("a/b.png", deliveryBody.get("key").getAsString());
      assertEquals(DELIVERY, c.getStorageDelivery(DELIVERY).get("id").getAsString());
      assertEquals(DELIVERY, c.retryStorageDelivery(DELIVERY).get("id").getAsString());
      assertArrayEquals("stored-bytes".getBytes(StandardCharsets.UTF_8), c.downloadStorageDelivery(DELIVERY));
      c.deleteStorageDestination(DESTINATION);

      assertEquals(ASSET, c.getAsset(ASSET, false).id());
      assertEquals("running", c.getJob(JOB, true).get("status").getAsString());

      var gone = assertThrows(EtchvClient.EtchvException.class, () -> c.getEmbedResult(JOB));
      assertEquals(410, gone.statusCode());
      assertTrue(gone.isGone());
      assertEquals(JOB, gone.requestId());
      assertTrue(gone.detail().contains("expired"));
      assertTrue(gone.getMessage().contains("410"));
      assertFalse(gone.toString().contains("test-key"));

      for (var invalid :
          List.<org.junit.jupiter.api.function.Executable>of(
              () -> c.updateWebhook("../x", true),
              () -> c.redeliverWebhook(WEBHOOK, "evt_../x"),
              () -> c.listWebhookDeliveries(WEBHOOK, "bad"),
              () -> c.verifyStorageDestination("dst_../x"),
              () -> c.getStorageDelivery(null),
              () -> c.createStorageDelivery(DESTINATION, "ast_bad", null),
              () -> c.submitEmbed("images", new byte[] {1}, Map.of("a", "b"), null, "wh_bad")))
        assertThrows(IllegalArgumentException.class, invalid);
    } finally {
      server.stop(0);
    }
    assertTrue(failures.isEmpty(), failures.toString());
    assertEquals(routes.size(), new HashSet<>(seen).size(), "Every route should be exercised");
  }

  static final String ASSET_FIXTURE_ID = "ast_" + "a".repeat(64);

  @Test
  void webhookSignatures() {
    byte[] body =
        "{\"id\":\"evt_x\",\"type\":\"watermark.embed.succeeded\"}".getBytes(StandardCharsets.UTF_8);
    String ts = "1790000000";
    // Reference value computed independently with Python hmac (see docs/api/webhooks).
    String sig = "v1=e5c745a91022ce84d157c997eca8c3e605c62efdfe52d651084fc97c94e0f060";
    Instant now = Instant.ofEpochSecond(1790000100);
    Duration tolerance = EtchvClient.WEBHOOK_TOLERANCE;
    assertTrue(EtchvClient.verifyWebhookSignature(body, ts, sig, SECRET, now, tolerance));
    assertFalse(EtchvClient.verifyWebhookSignature(body, ts, sig.replace('c', 'd'), SECRET, now, tolerance));
    assertFalse(EtchvClient.verifyWebhookSignature(body, ts, sig, SECRET + "x", now, tolerance));
    byte[] tampered = body.clone();
    tampered[2] = 'X';
    assertFalse(EtchvClient.verifyWebhookSignature(tampered, ts, sig, SECRET, now, tolerance));
    assertFalse(
        EtchvClient.verifyWebhookSignature(
            body, ts, sig, SECRET, Instant.ofEpochSecond(1790000301), tolerance));
    assertFalse(EtchvClient.verifyWebhookSignature(body, "-1", sig, SECRET, now, tolerance));
    assertFalse(EtchvClient.verifyWebhookSignature(body, null, sig, SECRET, now, tolerance));
    assertFalse(EtchvClient.verifyWebhookSignature(body, ts, sig, SECRET));
  }

  @Test
  void versionMatchesPom() throws Exception {
    var matcher =
        Pattern.compile("<artifactId>etchv-sdk</artifactId>\\s*<version>([^<]+)</version>")
            .matcher(Files.readString(Path.of("pom.xml")));
    assertTrue(matcher.find());
    assertEquals(matcher.group(1), EtchvClient.VERSION);
    assertEquals("etchv-java/" + matcher.group(1), EtchvClient.USER_AGENT);
  }

  @Test
  void exceptionsDoNotLeakDetail() {
    var e = new EtchvClient.EtchvException(401, "{\"detail\":\"secret-ish\"}", "req_1", "idem");
    assertEquals(401, e.statusCode());
    assertEquals("idem", e.idempotencyKey());
    assertFalse(e.getMessage().contains("secret-ish"));
    assertTrue(e.getMessage().contains("req_1"));
    assertTrue(new EtchvClient.EtchvException(0, "x", null, null).getMessage().contains("without an HTTP response"));
  }
}
