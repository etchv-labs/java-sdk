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
import org.junit.jupiter.api.Test;

final class AcceleratorTest {
  private static final String ID = "a".repeat(64), JOB = "req_" + "b".repeat(64);
  private static final byte[] PNG = {(byte) 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n', 0};

  private interface Handler {
    void handle(HttpExchange e) throws IOException;
  }

  private static <T> T withServer(Handler handler, ClientCall<T> call) throws Exception {
    var failures = new CopyOnWriteArrayList<Throwable>();
    var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        e -> {
          try {
            e.getRequestBody().readAllBytes();
            handler.handle(e);
          } catch (Throwable error) {
            failures.add(error);
            e.sendResponseHeaders(500, -1);
          } finally {
            e.close();
          }
        });
    server.start();
    try (var client =
        new EtchvClient(
            "test-key",
            "http://127.0.0.1:" + server.getAddress().getPort(),
            Duration.ofSeconds(5))) {
      T result = call.run(client);
      assertTrue(failures.isEmpty(), failures.toString());
      return result;
    } finally {
      server.stop(0);
    }
  }

  private interface ClientCall<T> {
    T run(EtchvClient client) throws Exception;
  }

  private static void send(HttpExchange e, int status, String type, byte[] body)
      throws IOException {
    e.getResponseHeaders().set("Content-Type", type);
    e.getResponseHeaders().set("X-Request-ID", JOB);
    e.sendResponseHeaders(status, body.length);
    e.getResponseBody().write(body);
  }

  private static byte[] detection(String accelerator) {
    return ("{\"watermarked\":true,\"confidence\":0.99,\"watermark_id\":\""
            + ID
            + "\""
            + (accelerator == null ? "" : ",\"accelerator\":\"" + accelerator + "\"")
            + "}")
        .getBytes(StandardCharsets.UTF_8);
  }

  @Test
  void acceleratorIsSentOnlyWhenSetAndResultReportsActualHardware() throws Exception {
    var queries = new CopyOnWriteArrayList<String>();
    var gpu = new EtchvClient.Options("photo.png", null).withAccelerator(EtchvClient.Accelerator.GPU);
    var result =
        withServer(
            e -> {
              queries.add(String.valueOf(e.getRequestURI().getRawQuery()));
              if (e.getRequestURI().getPath().endsWith("/detect")) {
                send(e, 200, "application/json", detection(null));
              } else {
                e.getResponseHeaders().set("X-Watermark-ID", ID);
                e.getResponseHeaders().set("X-Etchv-Accelerator", "gpu");
                send(e, 200, "image/png", PNG);
              }
            },
            c ->
                List.of(
                    c.embedImage(PNG, Map.of("a", 1), gpu),
                    c.embedImage(PNG, Map.of("a", 1), new EtchvClient.Options()),
                    c.detectImage(PNG, gpu)));
    assertEquals(List.of("accelerator=gpu", "null", "accelerator=gpu"), queries);
    assertEquals(
        EtchvClient.Accelerator.GPU, ((EtchvClient.EmbedResult) result.get(0)).accelerator());
    assertNull(((EtchvClient.DetectionResult) result.get(2)).accelerator());
  }

  @Test
  void acceleratorCombinesWithStorageAndWebhookParameters() throws Exception {
    var queries = new CopyOnWriteArrayList<String>();
    String webhook = "wh_" + "a".repeat(32), destination = "dst_" + "c".repeat(32);
    withServer(
        e -> {
          assertTrue(e.getRequestURI().getPath().endsWith("/async"));
          queries.add(e.getRequestURI().getRawQuery());
          send(
              e,
              202,
              "application/json",
              ("{\"status\":\"queued\",\"request_id\":\"" + JOB + "\"}")
                  .getBytes(StandardCharsets.UTF_8));
        },
        c -> {
          var options =
              new EtchvClient.Options("f.pdf", "stable_key", destination, "out.pdf")
                  .withAccelerator(EtchvClient.Accelerator.CPU);
          c.submitEmbed("documents", PNG, Map.of("a", 1), options, webhook);
          c.submitDetection(
              "videos",
              PNG,
              new EtchvClient.Options(null, null).withAccelerator(EtchvClient.Accelerator.GPU),
              null);
          return null;
        });
    assertEquals(
        List.of(
            "webhook_id="
                + webhook
                + "&storage_destination_id="
                + destination
                + "&storage_key=out.pdf&accelerator=cpu",
            "accelerator=gpu"),
        queries);
  }

  @Test
  void durableJobsPollResultWithoutRepeatingQueryAndReadJobAccelerator() throws Exception {
    var paths = new CopyOnWriteArrayList<String>();
    var result =
        withServer(
            e -> {
              paths.add(e.getRequestURI().toString());
              if (e.getRequestMethod().equals("POST")) {
                assertEquals("accelerator=gpu", e.getRequestURI().getRawQuery());
                e.getResponseHeaders().set("Retry-After", "0.01");
                send(
                    e,
                    202,
                    "application/json",
                    ("{\"request_id\":\"" + JOB + "\"}").getBytes(StandardCharsets.UTF_8));
              } else {
                assertNull(e.getRequestURI().getRawQuery());
                send(e, 200, "application/json", detection("cpu"));
              }
            },
            c ->
                c.detectVideo(
                    PNG,
                    new EtchvClient.Options("v.mp4", null)
                        .withAccelerator(EtchvClient.Accelerator.GPU)));
    assertEquals("/watermarks/detection-jobs/" + JOB + "/result", paths.get(1));
    assertEquals(EtchvClient.Accelerator.CPU, result.accelerator());
  }

  @Test
  void unknownAcceleratorDoesNotFailTheResponse() throws Exception {
    var result =
        withServer(
            e -> {
              e.getResponseHeaders().set("X-Watermark-ID", ID);
              e.getResponseHeaders().set("X-Etchv-Accelerator", "tpu");
              send(e, 200, "image/png", PNG);
            },
            c -> c.getEmbedResult(JOB));
    assertNull(result.accelerator());
  }

  @Test
  void rateLimitedDurableRequestsWaitForRetryAfter() throws Exception {
    var calls = new AtomicInteger();
    var times = new CopyOnWriteArrayList<Long>();
    var result =
        withServer(
            e -> {
              times.add(System.nanoTime());
              if (calls.incrementAndGet() == 1) {
                e.getResponseHeaders().set("Retry-After", "0.3");
                send(
                    e,
                    429,
                    "application/json",
                    "{\"detail\":{\"code\":\"rate_limited\"}}".getBytes(StandardCharsets.UTF_8));
              } else {
                e.getResponseHeaders().set("X-Watermark-ID", ID);
                send(e, 200, "image/png", PNG);
              }
            },
            c -> c.embedImage(PNG, Map.of("a", 1), null));
    assertEquals(ID, result.watermarkId());
    assertEquals(2, calls.get());
    assertTrue(times.get(1) - times.get(0) >= 250_000_000L, "waited for Retry-After");
  }

  @Test
  void rateLimitWaitsAreCappedAndNeverExceedTheClientDeadline() throws Exception {
    assertEquals(5, EtchvClient.retryDelaySeconds("60"));
    assertEquals(5, EtchvClient.retryDelaySeconds("3600"));
    assertEquals(.01, EtchvClient.retryDelaySeconds("0"));
    assertEquals(.3, EtchvClient.retryDelaySeconds("0.3"), 1e-9);
    assertEquals(1, EtchvClient.retryDelaySeconds(null));
    assertEquals(1, EtchvClient.retryDelaySeconds("soon"));
    assertEquals(Duration.ZERO, EtchvClient.parseRetryAfter("Wed, 21 Oct 2015 07:28:00 GMT"));
    assertNull(EtchvClient.parseRetryAfter("soon"));
    var failures = new CopyOnWriteArrayList<Throwable>();
    var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        e -> {
          try {
            e.getRequestBody().readAllBytes();
            e.getResponseHeaders().set("Retry-After", "60");
            send(e, 429, "application/json", "{}".getBytes(StandardCharsets.UTF_8));
          } catch (Throwable error) {
            failures.add(error);
          } finally {
            e.close();
          }
        });
    server.start();
    try (var c =
        new EtchvClient(
            "test-key",
            "http://127.0.0.1:" + server.getAddress().getPort(),
            Duration.ofMillis(300))) {
      long started = System.nanoTime();
      var error =
          assertThrows(
              EtchvClient.EtchvException.class, () -> c.embedImage(PNG, Map.of("a", 1), null));
      assertEquals(0, error.statusCode());
      assertTrue(System.nanoTime() - started < 5_000_000_000L);
      assertTrue(failures.isEmpty(), failures.toString());
    } finally {
      server.stop(0);
    }
  }

  @Test
  void rateLimitIsNotRetriedForNonDurableRequests() throws Exception {
    var calls = new AtomicInteger();
    var error =
        withServer(
            e -> {
              calls.incrementAndGet();
              e.getResponseHeaders().set("Retry-After", "0.01");
              send(e, 429, "application/json", "{}".getBytes(StandardCharsets.UTF_8));
            },
            c ->
                assertThrows(
                    EtchvClient.EtchvException.class, () -> c.detectImage(PNG, null)));
    assertEquals(429, error.statusCode());
    assertEquals(1, calls.get());
    assertEquals(Duration.ofMillis(10), error.retryAfter());
  }

  @Test
  void rateLimitErrorsExposeCodeMessageAndRetryAfter() throws Exception {
    String body =
        "{\"detail\":{\"message\":\"Too many requests\","
            + "\"code\":\"concurrency_limited\",\"limit\":2}}";
    var error =
        withServer(
            e -> {
              e.getResponseHeaders().set("Retry-After", "30");
              e.getResponseHeaders().set("X-RateLimit-Limit", "2");
              send(e, 429, "application/json", body.getBytes(StandardCharsets.UTF_8));
            },
            c -> assertThrows(EtchvClient.EtchvException.class, c::getApiKeyInfo));
    assertEquals(429, error.statusCode());
    assertEquals("concurrency_limited", error.code());
    assertEquals("Too many requests", error.detailMessage());
    assertEquals(2, error.limit());
    assertTrue(error.getMessage().startsWith("Etchv request failed (HTTP 429): Too many requests; request ID "));
    assertEquals(Duration.ofSeconds(30), error.retryAfter());
    assertEquals(body, error.detail());
    var withId =
        new EtchvClient.EtchvException(
            429, "{\"detail\":{\"message\":\"Slow down\",\"code\":\"rate_limited\",\"limit\":1.5}}", "req_9", null);
    assertEquals("Etchv request failed (HTTP 429): Slow down; request ID req_9", withId.getMessage());
    assertNull(withId.limit());

    var denied =
        withServer(
            e ->
                send(
                    e,
                    403,
                    "application/json",
                    "{\"detail\":\"GPU processing requires Business or a higher plan\"}"
                        .getBytes(StandardCharsets.UTF_8)),
            c -> assertThrows(EtchvClient.EtchvException.class, c::getApiKeyInfo));
    assertNull(denied.code());
    assertNull(denied.retryAfter());
    assertEquals("GPU processing requires Business or a higher plan", denied.detailMessage());
    assertNull(denied.limit());
    assertTrue(denied.getMessage().startsWith("Etchv request failed (HTTP 403); request ID "));

    var plain = new EtchvClient.EtchvException(500, "not json", null, null);
    assertNull(plain.code());
    assertNull(plain.detailMessage());
  }

  @Test
  void acceleratorWireValues() {
    assertEquals("gpu", EtchvClient.Accelerator.GPU.value());
    assertEquals(EtchvClient.Accelerator.CPU, EtchvClient.Accelerator.fromValue("CPU"));
    assertNull(EtchvClient.Accelerator.fromValue(null));
  }
}
