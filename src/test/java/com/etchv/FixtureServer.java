package com.etchv;

import static org.junit.jupiter.api.Assertions.*;

import com.google.gson.*;
import com.sun.net.httpserver.*;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

final class FixtureServer implements AutoCloseable {
  final String base;
  private final HttpServer server;
  private final JsonArray formats;
  private final Map<String, Integer> counts = new ConcurrentHashMap<>();
  private final Map<String, String> keys = new HashMap<>();
  private final List<Throwable> failures = new CopyOnWriteArrayList<>();
  private final String id = "a".repeat(64), job = "req_" + "b".repeat(64);

  FixtureServer(JsonArray formats) throws IOException {
    this.formats = formats;
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    base = "http://127.0.0.1:" + server.getAddress().getPort();
    server.createContext("/", this::handle);
    server.start();
  }

  private void handle(HttpExchange exchange) throws IOException {
    try {
      serve(exchange);
    } catch (Throwable e) {
      failures.add(e);
      try {
        exchange.sendResponseHeaders(500, -1);
      } catch (IOException ignored) {
      }
    } finally {
      exchange.close();
    }
  }

  private void serve(HttpExchange e) throws IOException {
    assertEquals("test-key", e.getRequestHeaders().getFirst("X-API-Key"));
    String[] parts = e.getRequestURI().getPath().substring(1).split("/");
    assertTrue(parts.length >= 3);
    String scenario = parts[0],
        ext = parts[1],
        route = String.join("/", Arrays.copyOfRange(parts, 2, parts.length)),
        method = e.getRequestMethod();
    JsonObject f = null;
    for (var entry : formats)
      if (entry.getAsJsonObject().get("extension").getAsString().equals(ext))
        f = entry.getAsJsonObject();
    assertNotNull(f);
    byte[] expected = Base64.getDecoder().decode(f.get("base64").getAsString());
    boolean detection = route.endsWith("/detect") || route.contains("detection-jobs/");
    String counter = scenario + "/" + ext + "/" + method;
    counts.merge(counter, 1, Integer::sum);
    if (method.equals("POST")) {
      byte[] body = e.getRequestBody().readAllBytes();
      boolean found = false;
      for (int i = 0; i <= body.length - expected.length; i++)
        if (Arrays.equals(body, i, i + expected.length, expected, 0, expected.length)) {
          found = true;
          break;
        }
      assertTrue(found);
      String text = new String(body, StandardCharsets.UTF_8);
      assertTrue(text.contains("filename=\"input." + ext + "\""));
      assertEquals(
          "watermarks/" + f.get("media").getAsString() + (detection ? "/detect" : ""), route);
      if (!detection) assertTrue(text.contains("{\"asset\":\"example\"}"));
      if (!detection || f.get("media").getAsString().equals("videos")) {
        String key = e.getRequestHeaders().getFirst("Idempotency-Key");
        assertNotNull(key);
        assertFalse(key.isBlank());
        String previous = keys.putIfAbsent(scenario + ext + route, key);
        if (previous != null) assertEquals(previous, key);
      }
    } else
      assertEquals(
          "watermarks/" + (detection ? "detection-jobs" : "jobs") + "/" + job + "/result", route);
    int status = 200;
    byte[] payload = expected;
    String mime = f.get("mime").getAsString();
    JsonObject json = null;
    var headers = e.getResponseHeaders();
    headers.set("X-Request-ID", job);
    switch (scenario) {
      case "retry":
        if (counts.get(counter) == 1) {
          status = 503;
          json = new JsonObject();
          json.addProperty("detail", "temporary");
        }
        break;
      case "failed":
        status = 503;
        json = new JsonObject();
        json.addProperty("status", "failed");
        break;
      case "redirect":
        status = 307;
        headers.set("Location", "/forbidden");
        json = new JsonObject();
        break;
      case "invalid":
        status = 422;
        json = new JsonObject();
        json.addProperty("detail", "unsupported profile");
        break;
      case "bad-job":
        status = 202;
        json = new JsonObject();
        json.addProperty("request_id", "../../forbidden");
        break;
      default:
        break;
    }
    if (json == null
        && List.of("embed-job", "detect-job", "deadline").contains(scenario)
        && (method.equals("POST") || scenario.equals("deadline"))) {
      status = 202;
      headers.set("Retry-After", "0.01");
      headers.set("Location", "https://untrusted.example/steal");
      json = new JsonObject();
      json.addProperty("request_id", job);
      json.addProperty("result_url", "https://untrusted.example/steal");
    } else if (json == null && detection) {
      json = new JsonObject();
      json.addProperty("watermarked", true);
      json.addProperty("confidence", scenario.equals("bad-detection") ? 1.5 : .99);
      json.addProperty("watermark_id", id);
      var units = new JsonArray();
      for (int i = 0; i < 2; i++) {
        var u = new JsonObject();
        u.addProperty("index", scenario.equals("bad-units") && i == 1 ? 4 : i);
        u.addProperty("watermarked", true);
        u.addProperty("confidence", .99);
        u.addProperty("watermark_id", id);
        units.add(u);
      }
      json.add("units", units);
    }
    if (json != null) {
      payload = json.toString().getBytes(StandardCharsets.UTF_8);
      mime = "application/json";
    } else {
      headers.set("X-Watermark-ID", scenario.equals("bad-id") ? "invalid" : id);
      headers.set("Content-Disposition", "attachment; filename=\"protected." + ext + "\"");
    }
    headers.set("Content-Type", mime);
    e.sendResponseHeaders(status, payload.length);
    try {
      e.getResponseBody().write(payload);
    } catch (IOException ignored) {
    }
  }

  void verify() {
    assertTrue(failures.isEmpty(), failures.toString());
    for (var f : formats)
      assertEquals(
          2, counts.get("formats/" + f.getAsJsonObject().get("extension").getAsString() + "/POST"));
    assertEquals(2, counts.get("retry/png/POST"));
    assertEquals(1, counts.get("failed/png/POST"));
    assertTrue(counts.get("embed-job/pdf/GET") >= 1);
    assertTrue(counts.get("detect-job/mp4/GET") >= 1);
  }

  public void close() {
    server.stop(0);
    assertTrue(failures.isEmpty(), failures.toString());
  }
}
