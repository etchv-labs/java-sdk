package com.etchv;

import com.google.gson.*;
import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.Flow;
import java.util.regex.Pattern;

/** Thread-safe, synchronous server-side client. Close it to release its HTTP pool. */
public final class EtchvClient implements AutoCloseable {
  public static final int MAX_FILE_SIZE = 20 * 1024 * 1024;

  public static final class EtchvException extends RuntimeException {
    public final int statusCode;
    public final String detail, requestId, idempotencyKey;

    public EtchvException(int status, String detail, String requestId, String key) {
      super("Etchv request failed (HTTP " + status + ")");
      this.statusCode = status;
      this.detail = detail;
      this.requestId = requestId;
      this.idempotencyKey = key;
    }
  }

  public record Options(String filename, String idempotencyKey, String storageDestinationId, String storageKey) {
    public Options(String filename, String idempotencyKey) { this(filename, idempotencyKey, null, null); }
    public Options() {
      this(null, null);
    }
  }

  public record EmbedResult(
      byte[] bytes, String watermarkId, String requestId, String contentType, String filename, String assetId, String sourceAssetId, String storageDeliveryId) {}

  public record DetectionUnit(
      int index, boolean watermarked, double confidence, String watermarkId) {}

  public record DetectionResult(
      boolean watermarked,
      double confidence,
      String watermarkId,
      String requestId,
      List<DetectionUnit> units) {}

  private final HttpClient http;
  private final String key, base;
  private final Duration timeout;
  private final Gson gson = new Gson();

  private static boolean validId(String id) {
    return id != null && id.matches("[0-9a-fA-F]{64}");
  }

  private static boolean validJob(String id) {
    return id != null && id.matches("req_[a-f0-9]{64}");
  }

  public EtchvClient(String apiKey) {
    this(apiKey, "https://api.etchv.com", Duration.ofMinutes(2));
  }

  public EtchvClient(String apiKey, String baseUrl, Duration timeout) {
    URI u = URI.create(baseUrl);
    if (apiKey == null
        || apiKey.isBlank()
        || apiKey.contains("\r")
        || apiKey.contains("\n")
        || timeout == null
        || timeout.isNegative()
        || timeout.isZero())
      throw new IllegalArgumentException("API key and positive timeout are required");
    if (u.getHost() == null
        || u.getUserInfo() != null
        || u.getQuery() != null
        || u.getFragment() != null
        || !("https".equals(u.getScheme())
            || ("http".equals(u.getScheme())
                && Set.of("localhost", "127.0.0.1", "[::1]").contains(u.getHost()))))
      throw new IllegalArgumentException("Base URL must use HTTPS (HTTP allowed for localhost)");
    this.key = apiKey;
    this.base = baseUrl.replaceAll("/+$", "");
    this.timeout = timeout;
    http = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();
  }

  @Override
  public void close() {
    http.close();
  }

  public EmbedResult embedImage(byte[] file, Map<String, ?> data, Options options)
      throws InterruptedException {
    return embed("images", file, data, options);
  }

  public EmbedResult embedDocument(byte[] file, Map<String, ?> data, Options options)
      throws InterruptedException {
    return embed("documents", file, data, options);
  }

  public EmbedResult embedVideo(byte[] file, Map<String, ?> data, Options options)
      throws InterruptedException {
    return embed("videos", file, data, options);
  }

  public DetectionResult detectImage(byte[] file, Options options) throws InterruptedException {
    return detect("images", file, options);
  }

  public DetectionResult detectDocument(byte[] file, Options options) throws InterruptedException {
    return detect("documents", file, options);
  }

  public DetectionResult detectVideo(byte[] file, Options options) throws InterruptedException {
    return detect("videos", file, options);
  }

  public record Asset(String id, String name, String kind, String mediaType, String format, String contentType,
      long sizeBytes, String sha256, String parentAssetId, String requestId, String watermarkId, String createdAt,
      String updatedAt, String fileExpiresAt, boolean fileAvailable, int version, JsonObject metadata, String downloadUrl,
      String storageProvider, String storageStatus, String storageDestinationId, String storageDeliveryId,
      String stagingExpiresAt, String stagingDeletedAt) {}
  public record AssetPage(List<Asset> items, String nextCursor) {}
  private static final Gson ASSET_JSON = new GsonBuilder().serializeNulls().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
  private static String assetPath(String id) {
    if (id == null || !id.matches("ast_[a-f0-9]{64}")) throw new IllegalArgumentException("Invalid asset ID");
    return "assets/" + id;
  }
  private byte[] assetRequest(String path, String method, Object body) throws InterruptedException {
    var builder = HttpRequest.newBuilder(URI.create(base + "/" + path)).header("X-API-Key", key).timeout(timeout);
    if (body != null) builder.header("Content-Type", "application/json");
    builder.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(ASSET_JSON.toJson(body)));
    try {
      var response = http.send(builder.build(), ignored -> new LimitedBody());
      int status = response.statusCode();
      if (status != 200 && status != 204) throw new EtchvException(status, new String(response.body(), 0, Math.min(response.body().length, 10000), StandardCharsets.UTF_8), response.headers().firstValue("X-Request-ID").orElse(null), null);
      return response.body();
    } catch (IOException e) { throw new EtchvException(0, e.getMessage(), null, null); }
  }
  public AssetPage listAssets(Map<String, String> options) throws InterruptedException {
    var query = new StringBuilder("assets?");
    for (var entry : options.entrySet()) {
      if (!Set.of("limit", "cursor", "kind", "media_type", "watermark_id").contains(entry.getKey())) throw new IllegalArgumentException("Unknown asset filter");
      query.append(entry.getKey()).append("=").append(java.net.URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8)).append("&");
    }
    return ASSET_JSON.fromJson(new String(assetRequest(query.toString(), "GET", null), StandardCharsets.UTF_8), AssetPage.class);
  }
  public Asset getAsset(String id) throws InterruptedException { return ASSET_JSON.fromJson(new String(assetRequest(assetPath(id), "GET", null), StandardCharsets.UTF_8), Asset.class); }
  public Asset updateAsset(String id, int version, Map<String, ?> changes) throws InterruptedException {
    var body = new HashMap<String, Object>(changes); body.put("version", version);
    return ASSET_JSON.fromJson(new String(assetRequest(assetPath(id), "PATCH", body), StandardCharsets.UTF_8), Asset.class);
  }
  public void deleteAsset(String id) throws InterruptedException { assetRequest(assetPath(id), "DELETE", null); }
  public void deleteAssets(List<String> ids) throws InterruptedException {
    if (ids.isEmpty() || ids.size() > 50) throw new IllegalArgumentException("Provide 1–50 asset IDs"); ids.forEach(EtchvClient::assetPath);
    assetRequest("assets/bulk-delete", "POST", Map.of("asset_ids", ids));
  }
  public JsonObject getStorageDelivery(String id) throws InterruptedException {
    if (!id.matches("std_[a-f0-9]{64}")) throw new IllegalArgumentException("Invalid storage delivery ID");
    return ASSET_JSON.fromJson(new String(assetRequest("storage/deliveries/" + id, "GET", null), StandardCharsets.UTF_8), JsonObject.class);
  }
  public byte[] downloadAsset(String id) throws InterruptedException { return assetRequest(assetPath(id) + "/content", "GET", null); }

  public JsonObject submitEmbed(String media, byte[] file, Map<String, ?> data, Options options, String webhookId) throws InterruptedException {
    if (data == null || data.isEmpty()) throw new IllegalArgumentException("data must be a non-empty JSON object");
    return submit(media, file, ASSET_JSON.toJson(data), options, webhookId);
  }
  public JsonObject submitDetection(String media, byte[] file, Options options, String webhookId) throws InterruptedException {
    return submit(media, file, null, options, webhookId);
  }
  private JsonObject submit(String media, byte[] file, String data, Options options, String webhookId) throws InterruptedException {
    if (!List.of("images", "documents", "videos").contains(media) || file == null || file.length == 0 || file.length > MAX_FILE_SIZE) throw new IllegalArgumentException("Invalid media or file size");
    if (webhookId != null && !webhookId.matches("wh_[a-f0-9]{32}")) throw new IllegalArgumentException("Invalid webhook ID");
    if (options == null) options = new Options();
    String idempotency = options.idempotencyKey();
    if (idempotency == null || idempotency.isEmpty()) idempotency = UUID.randomUUID().toString();
    var r = request("watermarks/" + media + (data == null ? "/detect" : "") + "/async" + (webhookId == null ? "" : "?webhook_id=" + webhookId), file, data, new Options(options.filename() == null ? "file" : options.filename(), idempotency, options.storageDestinationId(), options.storageKey()), true, data == null);
    return JsonParser.parseString(new String(r.body(), StandardCharsets.UTF_8)).getAsJsonObject();
  }
  public JsonObject getJob(String requestId, boolean detect) throws InterruptedException {
    if (!validJob(requestId)) throw new IllegalArgumentException("Invalid request ID");
    var r = request("watermarks/" + (detect ? "detection-jobs" : "jobs") + "/" + requestId, null, null, new Options(), false, detect);
    return JsonParser.parseString(new String(r.body(), StandardCharsets.UTF_8)).getAsJsonObject();
  }

  public EmbedResult getEmbedResult(String requestId) throws InterruptedException {
    if (!validJob(requestId)) throw new IllegalArgumentException("Invalid request ID");
    return embedding(
        request(
            "watermarks/jobs/" + requestId + "/result", null, null, new Options(), true, false));
  }

  public DetectionResult getDetectionResult(String requestId) throws InterruptedException {
    if (!validJob(requestId)) throw new IllegalArgumentException("Invalid request ID");
    return detection(
        request(
            "watermarks/detection-jobs/" + requestId + "/result",
            null,
            null,
            new Options(),
            true,
            true));
  }

  private EmbedResult embed(String media, byte[] file, Map<String, ?> data, Options options)
      throws InterruptedException {
    if (data == null || data.isEmpty())
      throw new IllegalArgumentException("data must be a non-empty JSON object");
    return embedding(post(media, file, gson.toJson(data), options));
  }

  private DetectionResult detect(String media, byte[] file, Options options)
      throws InterruptedException {
    return detection(post(media, file, null, options));
  }

  private HttpResponse<byte[]> post(String media, byte[] file, String data, Options options)
      throws InterruptedException {
    if (file == null || file.length == 0 || file.length > MAX_FILE_SIZE)
      throw new IllegalArgumentException("file must contain 1 byte to 20 MB");
    if (options == null) options = new Options();
    boolean durable = data != null || media.equals("videos");
    String filename =
        options.filename() != null
            ? options.filename()
            : switch (media) {
              case "documents" -> "document.pdf";
              case "videos" -> "video.mp4";
              default -> "image.png";
            };
    String idempotency = options.idempotencyKey();
    if (durable && (idempotency == null || idempotency.isEmpty()))
      idempotency = UUID.randomUUID().toString();
    return request(
        "watermarks/" + media + (data == null ? "/detect" : ""),
        file,
        data,
        new Options(filename, idempotency, options.storageDestinationId(), options.storageKey()),
        durable,
        data == null && media.equals("videos"));
  }

  private static byte[] multipart(byte[] file, String data, String filename, String boundary) {
    var out = new ByteArrayOutputStream();
    String safe =
        filename.replace("\r", "_").replace("\n", "_").replace("\"", "_").replace("\\", "_");
    out.writeBytes(
        ("--"
                + boundary
                + "\r\nContent-Disposition: form-data; name=\"file\"; filename=\""
                + safe
                + "\"\r\nContent-Type: application/octet-stream\r\n\r\n")
            .getBytes(StandardCharsets.UTF_8));
    out.writeBytes(file);
    out.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
    if (data != null)
      out.writeBytes(
          ("--"
                  + boundary
                  + "\r\nContent-Disposition: form-data; name=\"data\"\r\n\r\n"
                  + data
                  + "\r\n")
              .getBytes(StandardCharsets.UTF_8));
    out.writeBytes(("--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));
    return out.toByteArray();
  }

  // Cancel oversized bodies before buffering the entire response.
  private static final class LimitedBody implements HttpResponse.BodySubscriber<byte[]> {
    private final CompletableFuture<byte[]> result = new CompletableFuture<>();
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    private Flow.Subscription subscription;

    public CompletionStage<byte[]> getBody() {
      return result;
    }

    public void onSubscribe(Flow.Subscription s) {
      subscription = s;
      s.request(1);
    }

    public void onNext(List<ByteBuffer> items) {
      for (var item : items) {
        if ((long) bytes.size() + item.remaining() > MAX_FILE_SIZE) {
          subscription.cancel();
          result.completeExceptionally(new IOException("Response exceeds 20 MB"));
          return;
        }
        byte[] chunk = new byte[item.remaining()];
        item.get(chunk);
        bytes.writeBytes(chunk);
      }
      subscription.request(1);
    }

    public void onError(Throwable e) {
      result.completeExceptionally(e);
    }

    public void onComplete() {
      result.complete(bytes.toByteArray());
    }
  }

  private HttpResponse<byte[]> request(
      String path, byte[] file, String data, Options options, boolean durable, boolean detectionJob)
      throws InterruptedException {
    if (options.storageKey() != null && options.storageDestinationId() == null) throw new IllegalArgumentException("Storage key requires destination");
    if (options.storageDestinationId() != null) {
      if (data == null || !options.storageDestinationId().matches("dst_[a-f0-9]{32}")) throw new IllegalArgumentException("Invalid storage destination or detection request");
      path += (path.contains("?") ? "&" : "?") + "storage_destination_id=" + java.net.URLEncoder.encode(options.storageDestinationId(), StandardCharsets.UTF_8);
      if (options.storageKey() != null) path += "&storage_key=" + java.net.URLEncoder.encode(options.storageKey(), StandardCharsets.UTF_8);
    }
    long started = System.nanoTime();
    String requestId = null;
    String boundary = "etchv-" + UUID.randomUUID();
    byte[] body = file == null ? null : multipart(file, data, options.filename(), boundary);
    while (System.nanoTime() - started < timeout.toNanos()) {
      if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
      var builder =
          HttpRequest.newBuilder(URI.create(base + "/" + path))
              .header("X-API-Key", key)
              .timeout(
                  Duration.ofNanos(Math.max(1, timeout.toNanos() - (System.nanoTime() - started))));
      if (options.idempotencyKey() != null)
        builder.header("Idempotency-Key", options.idempotencyKey());
      if (body != null)
        builder
            .header("Content-Type", "multipart/form-data; boundary=" + boundary)
            .POST(HttpRequest.BodyPublishers.ofByteArray(body));
      else builder.GET();
      HttpResponse<byte[]> response;
      try {
        response = http.send(builder.build(), ignored -> new LimitedBody());
      } catch (IOException e) {
        if (!durable)
          throw new EtchvException(0, e.getMessage(), requestId, options.idempotencyKey());
        pause(started, 1);
        continue;
      }
      requestId = response.headers().firstValue("X-Request-ID").orElse(requestId);
      int status = response.statusCode();
      if (status == 200 || (status == 202 && path.split("\\?", 2)[0].endsWith("/async"))) return response;
      JsonObject detail = new JsonObject();
      try {
        var value = JsonParser.parseString(new String(response.body(), StandardCharsets.UTF_8));
        if (value.isJsonObject()) detail = value.getAsJsonObject();
      } catch (JsonParseException ignored) {
      }
      if (durable && status == 202) {
        var id = detail.get("request_id");
        if (id == null
            || !id.isJsonPrimitive()
            || !id.getAsJsonPrimitive().isString()
            || !validJob(id.getAsString()))
          throw new EtchvException(
              202, "Invalid job response", requestId, options.idempotencyKey());
        requestId = id.getAsString();
        path =
            "watermarks/"
                + (detectionJob ? "detection-jobs" : "jobs")
                + "/"
                + requestId
                + "/result";
        body = null;
        double delay = 1;
        try {
          delay = Double.parseDouble(response.headers().firstValue("Retry-After").orElse("1"));
        } catch (NumberFormatException ignored) {
        }
        pause(started, Double.isFinite(delay) ? Math.max(.01, Math.min(5, delay)) : 1);
        continue;
      }
      if (durable
          && Set.of(429, 502, 503, 504).contains(status)
          && !new JsonPrimitive("failed").equals(detail.get("status"))) {
        pause(started, 1);
        continue;
      }
      throw new EtchvException(
          status,
          new String(
              response.body(), 0, Math.min(10000, response.body().length), StandardCharsets.UTF_8),
          requestId,
          options.idempotencyKey());
    }
    throw new EtchvException(
        0, "Client deadline exceeded; job may still complete", requestId, options.idempotencyKey());
  }

  private void pause(long started, double seconds) throws InterruptedException {
    long remaining = timeout.toNanos() - (System.nanoTime() - started);
    if (remaining > 0) TimeUnit.NANOSECONDS.sleep(Math.min(remaining, (long) (seconds * 1e9)));
  }

  private static EmbedResult embedding(HttpResponse<byte[]> r) {
    String mime = r.headers().firstValue("Content-Type").orElse("").split(";")[0],
        id = r.headers().firstValue("X-Watermark-ID").orElse(null),
        requestId = r.headers().firstValue("X-Request-ID").orElse(null);
    String ext = extension(r.body(), mime);
    if (ext == null || !validId(id))
      throw new EtchvException(200, "Invalid embedding response", requestId, null);
    var match =
        Pattern.compile("filename=\"([A-Za-z0-9._-]+)\"")
            .matcher(r.headers().firstValue("Content-Disposition").orElse(""));
    return new EmbedResult(
        r.body(), id, requestId, mime, match.find() ? match.group(1) : "watermarked." + ext,
        r.headers().firstValue("X-Asset-ID").orElse(null), r.headers().firstValue("X-Source-Asset-ID").orElse(null), r.headers().firstValue("X-Storage-Delivery-ID").orElse(null));
  }

  private static DetectionUnit unit(JsonObject v, int index) {
    var w = v.get("watermarked");
    var c = v.get("confidence");
    var id = v.get("watermark_id");
    if (w == null
        || !w.isJsonPrimitive()
        || !w.getAsJsonPrimitive().isBoolean()
        || c == null
        || !c.isJsonPrimitive()
        || !c.getAsJsonPrimitive().isNumber()
        || id == null) throw new IllegalArgumentException();
    double confidence = c.getAsDouble();
    boolean marked = w.getAsBoolean();
    if (!Double.isFinite(confidence)
        || confidence < 0
        || confidence > 1
        || (marked
            ? !id.isJsonPrimitive()
                || !id.getAsJsonPrimitive().isString()
                || !validId(id.getAsString())
            : !id.isJsonNull())) throw new IllegalArgumentException();
    return new DetectionUnit(index, marked, confidence, marked ? id.getAsString() : null);
  }

  private static DetectionResult detection(HttpResponse<byte[]> r) {
    String requestId = r.headers().firstValue("X-Request-ID").orElse(null);
    try {
      var v =
          JsonParser.parseString(new String(r.body(), StandardCharsets.UTF_8)).getAsJsonObject();
      var top = unit(v, 0);
      var units = new ArrayList<DetectionUnit>();
      if (v.has("units")) {
        for (var value : v.getAsJsonArray("units")) {
          var u = value.getAsJsonObject();
          var index = u.get("index");
          if (index == null
              || !index.isJsonPrimitive()
              || !index.getAsJsonPrimitive().isNumber()
              || index.getAsBigDecimal().compareTo(java.math.BigDecimal.valueOf(units.size())) != 0)
            throw new IllegalArgumentException();
          units.add(unit(u, units.size()));
        }
        if (units.isEmpty()) throw new IllegalArgumentException();
      } else units.add(top);
      return new DetectionResult(
          top.watermarked(), top.confidence(), top.watermarkId(), requestId, List.copyOf(units));
    } catch (RuntimeException e) {
      throw new EtchvException(200, "Invalid detection response", requestId, null);
    }
  }

  private static String extension(byte[] b, String mime) {
    String s = new String(b, 0, Math.min(b.length, 12), StandardCharsets.ISO_8859_1);
    return switch (mime) {
      case "image/png" -> s.startsWith("\u0089PNG\r\n\u001a\n") ? "png" : null;
      case "image/jpeg" -> s.startsWith("\u00ff\u00d8\u00ff") ? "jpg" : null;
      case "image/gif" -> s.startsWith("GIF87a") || s.startsWith("GIF89a") ? "gif" : null;
      case "image/tiff" -> s.startsWith("II*\0") || s.startsWith("MM\0*") ? "tiff" : null;
      case "image/bmp" -> s.startsWith("BM") ? "bmp" : null;
      case "image/x-portable-pixmap" -> s.startsWith("P6") || s.startsWith("P3") ? "ppm" : null;
      case "image/webp" ->
          s.startsWith("RIFF") && s.length() >= 12 && s.substring(8, 12).equals("WEBP")
              ? "webp"
              : null;
      case "image/vnd.adobe.photoshop" ->
          s.startsWith("8BPS\0\1") ? "psd" : s.startsWith("8BPS\0\2") ? "psb" : null;
      case "application/pdf" -> s.startsWith("%PDF-") ? "pdf" : null;
      case "video/mp4", "video/quicktime" ->
          s.length() >= 8 && s.substring(4, 8).equals("ftyp")
              ? (mime.equals("video/mp4") ? "mp4" : "mov")
              : null;
      default -> null;
    };
  }
}
