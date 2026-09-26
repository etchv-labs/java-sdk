package com.etchv;

import com.google.gson.*;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import java.io.*;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.Flow;
import java.util.regex.Pattern;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Thread-safe, synchronous server-side client for the Etchv API.
 *
 * <p>Requests authenticate with the {@code X-API-Key} header. Keep API keys on your server. The
 * client owns an HTTP connection pool; close it (for example with try-with-resources) to release
 * it.
 *
 * <p>All request methods are blocking. They throw {@link EtchvException} for API and transport
 * failures, {@link IllegalArgumentException} for invalid arguments detected before a request is
 * sent, and {@link InterruptedException} when the calling thread is interrupted.
 *
 * <pre>{@code
 * try (var client = new EtchvClient(System.getenv("ETCHV_API_KEY"))) {
 *   var result = client.embedImage(bytes, Map.of("asset", "photo-123"),
 *       new EtchvClient.Options("photo.jpg", null));
 * }
 * }</pre>
 */
public final class EtchvClient implements AutoCloseable {
  /** SDK version, also sent in the {@code User-Agent} header. */
  public static final String VERSION = "1.0.0";

  /** {@code User-Agent} header value sent with every request. */
  public static final String USER_AGENT = "etchv-java/" + VERSION;

  /** Default API base URL. */
  public static final String DEFAULT_BASE_URL = "https://api.etchv.com";

  /** Default client deadline for a single method call. */
  public static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(2);

  /** Maximum upload and response size, in bytes (20 MB). */
  public static final int MAX_FILE_SIZE = 20 * 1024 * 1024;

  /** Maximum age of a webhook timestamp accepted by {@link #verifyWebhookSignature}. */
  public static final Duration WEBHOOK_TOLERANCE = Duration.ofMinutes(5);

  /**
   * Unchecked exception for a failed Etchv request.
   *
   * <p>{@link #statusCode()} is the HTTP status, or {@code 0} when no HTTP response was received
   * (network failure or client deadline). The message never contains the API key or the response
   * body; the bounded response body is available from {@link #detail()}.
   */
  public static final class EtchvException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /** HTTP status code, or {@code 0} for a transport failure or client deadline. */
    public final int statusCode;

    /** Bounded response body (at most 10,000 characters) or a client-side reason; may be null. */
    public final String detail;

    /** Etchv request ID ({@code X-Request-ID} or job ID) when known; may be null. */
    public final String requestId;

    /** Idempotency key sent with the request, when any; may be null. */
    public final String idempotencyKey;

    /**
     * Creates an exception.
     *
     * @param status HTTP status code, or 0 for a transport failure
     * @param detail bounded response body or client-side reason
     * @param requestId Etchv request ID, when known
     * @param key idempotency key sent with the request, when any
     */
    public EtchvException(int status, String detail, String requestId, String key) {
      this(status, detail, requestId, key, null);
    }

    /**
     * Creates an exception with an underlying cause.
     *
     * @param status HTTP status code, or 0 for a transport failure
     * @param detail bounded response body or client-side reason
     * @param requestId Etchv request ID, when known
     * @param key idempotency key sent with the request, when any
     * @param cause underlying transport failure, or null
     */
    public EtchvException(int status, String detail, String requestId, String key, Throwable cause) {
      super(
          (status == 0
                  ? "Etchv request failed without an HTTP response"
                  : "Etchv request failed (HTTP " + status + ")")
              + (requestId == null ? "" : "; request ID " + requestId),
          cause);
      this.statusCode = status;
      this.detail = detail;
      this.requestId = requestId;
      this.idempotencyKey = key;
    }

    /**
     * Returns the HTTP status code.
     *
     * @return HTTP status code, or 0 when no HTTP response was received
     */
    public int statusCode() {
      return statusCode;
    }

    /**
     * Returns the bounded response body or client-side reason.
     *
     * @return detail text, or null
     */
    public String detail() {
      return detail;
    }

    /**
     * Returns the Etchv request or job ID.
     *
     * @return request ID, or null when unknown
     */
    public String requestId() {
      return requestId;
    }

    /**
     * Returns the idempotency key sent with the request.
     *
     * @return idempotency key, or null
     */
    public String idempotencyKey() {
      return idempotencyKey;
    }

    /**
     * Whether a saved job result or asset file expired or was deleted (HTTP 410). Retrying with
     * the same idempotency key does not charge again.
     *
     * @return true for HTTP 410
     */
    public boolean isGone() {
      return statusCode == 410;
    }
  }

  /**
   * Per-request options for media operations.
   *
   * @param filename original filename sent with the upload; a media default is used when null
   * @param idempotencyKey key for safe retries (8–128 letters, digits, hyphens or underscores);
   *     generated when null for durable operations
   * @param storageDestinationId verified customer storage destination ({@code dst_…}) for
   *     embedding results; null uses Etchv storage
   * @param storageKey optional relative object key beneath the destination prefix; requires a
   *     destination
   */
  public record Options(
      String filename, String idempotencyKey, String storageDestinationId, String storageKey) {
    /**
     * Options without a storage destination.
     *
     * @param filename original filename, or null
     * @param idempotencyKey idempotency key, or null
     */
    public Options(String filename, String idempotencyKey) {
      this(filename, idempotencyKey, null, null);
    }

    /** Default options. */
    public Options() {
      this(null, null);
    }
  }

  /**
   * A verified watermarked file.
   *
   * @param bytes watermarked file in its original format
   * @param watermarkId 64-character hexadecimal watermark ID
   * @param requestId Etchv request ID
   * @param contentType MIME type of {@code bytes}
   * @param filename suggested filename
   * @param assetId watermarked asset ID, when recorded
   * @param sourceAssetId original upload asset ID, when recorded
   * @param storageDeliveryId customer storage delivery ID, when a destination was selected
   */
  public record EmbedResult(
      byte[] bytes,
      String watermarkId,
      String requestId,
      String contentType,
      String filename,
      String assetId,
      String sourceAssetId,
      String storageDeliveryId) {}

  /**
   * Detection result for one frame, page or composite.
   *
   * @param index zero-based unit index
   * @param watermarked whether a watermark was found in this unit
   * @param confidence certainty between 0 and 1
   * @param watermarkId decoded watermark ID, or null
   */
  public record DetectionUnit(
      int index, boolean watermarked, double confidence, String watermarkId) {}

  /**
   * Detection result for a file.
   *
   * @param watermarked whether every unit carries the same watermark
   * @param confidence lowest per-unit confidence
   * @param watermarkId decoded watermark ID when all units agree, otherwise null
   * @param requestId Etchv request ID
   * @param units per-unit results
   */
  public record DetectionResult(
      boolean watermarked,
      double confidence,
      String watermarkId,
      String requestId,
      List<DetectionUnit> units) {}

  /**
   * An asset library record.
   *
   * @param id asset ID ({@code ast_…})
   * @param name display name
   * @param kind {@code source} or {@code watermarked}
   * @param mediaType {@code image}, {@code document} or {@code video}
   * @param format file format, for example {@code PNG}
   * @param contentType MIME type
   * @param sizeBytes file size
   * @param sha256 file SHA-256
   * @param parentAssetId original asset for a watermarked output, or null
   * @param requestId job request ID
   * @param watermarkId watermark ID, or null
   * @param createdAt creation time (ISO 8601)
   * @param updatedAt last update time (ISO 8601)
   * @param fileExpiresAt Etchv storage file expiry, or null
   * @param fileAvailable whether the file can be downloaded
   * @param version current version for optimistic updates
   * @param metadata user metadata, or null
   * @param downloadUrl authenticated relative download path, or null
   * @param storageProvider {@code etchv}, {@code s3}, {@code gcs} or {@code azure}
   * @param storageStatus storage delivery state
   * @param storageDestinationId customer storage destination, or null
   * @param storageDeliveryId customer storage delivery, or null
   * @param stagingExpiresAt temporary Etchv copy expiry, or null
   * @param stagingDeletedAt when the temporary Etchv copy was removed, or null
   */
  public record Asset(
      String id,
      String name,
      String kind,
      String mediaType,
      String format,
      String contentType,
      long sizeBytes,
      String sha256,
      String parentAssetId,
      String requestId,
      String watermarkId,
      String createdAt,
      String updatedAt,
      String fileExpiresAt,
      boolean fileAvailable,
      int version,
      JsonObject metadata,
      String downloadUrl,
      String storageProvider,
      String storageStatus,
      String storageDestinationId,
      String storageDeliveryId,
      String stagingExpiresAt,
      String stagingDeletedAt) {}

  /**
   * A page of assets.
   *
   * @param items assets, newest first
   * @param nextCursor cursor for the next page, or null at the end
   */
  public record AssetPage(List<Asset> items, String nextCursor) {}

  /**
   * Identity of the configured API key, from {@link #getApiKeyInfo()}.
   *
   * @param organizationId organization that owns the key
   * @param keyId key ID ({@code key_…}); not the secret
   * @param scopes granted scopes
   */
  public record ApiKeyInfo(String organizationId, String keyId, List<String> scopes) {}

  /**
   * A webhook endpoint.
   *
   * @param id endpoint ID ({@code wh_…})
   * @param url HTTPS delivery URL
   * @param enabled whether deliveries are sent
   * @param createdAt creation time (ISO 8601)
   * @param signingSecret one-time signing secret, returned only by {@link #createWebhook}; null
   *     otherwise. Never included in {@link #toString()}.
   */
  public record WebhookEndpoint(
      String id, String url, boolean enabled, String createdAt, String signingSecret) {
    @Override
    public String toString() {
      return "WebhookEndpoint[id="
          + id
          + ", url="
          + url
          + ", enabled="
          + enabled
          + ", createdAt="
          + createdAt
          + ", signingSecret="
          + (signingSecret == null ? "null" : "<redacted>")
          + "]";
    }
  }

  /**
   * A webhook event delivery record.
   *
   * @param id event ID ({@code evt_…})
   * @param requestId job request ID
   * @param status {@code queued}, {@code delivering}, {@code retrying}, {@code delivered}, {@code
   *     exhausted} or {@code cancelled}
   * @param attempts attempts made
   * @param createdAt creation time (ISO 8601)
   * @param nextAttemptAt next scheduled attempt (ISO 8601)
   * @param history recent attempts with {@code at}, {@code status_code} and {@code error}
   * @param payload the event JSON body
   */
  public record WebhookDelivery(
      String id,
      String requestId,
      String status,
      int attempts,
      String createdAt,
      String nextAttemptAt,
      JsonArray history,
      JsonObject payload) {}

  /**
   * A page of webhook deliveries.
   *
   * @param items deliveries, newest first
   * @param nextCursor event ID to pass as {@code after} for the next page, or null at the end
   */
  public record WebhookDeliveryPage(
      @SerializedName("data") List<WebhookDelivery> items, String nextCursor) {}

  private static final Gson JSON =
      new GsonBuilder()
          .serializeNulls()
          .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
          .create();
  // Forensic data is serialized exactly as given: no field renaming, nulls preserved.
  private static final Gson DATA_JSON = new GsonBuilder().serializeNulls().create();
  private static final Pattern WATERMARK_ID = Pattern.compile("[0-9a-fA-F]{64}");
  private static final Pattern JOB_ID = Pattern.compile("req_[a-f0-9]{64}");
  private static final Pattern ASSET_ID = Pattern.compile("ast_[a-f0-9]{64}");
  private static final Pattern WEBHOOK_ID = Pattern.compile("wh_[a-f0-9]{32}");
  private static final Pattern EVENT_ID = Pattern.compile("evt_[a-f0-9]{64}");
  private static final Pattern DESTINATION_ID = Pattern.compile("dst_[a-f0-9]{32}");
  private static final Pattern DELIVERY_ID = Pattern.compile("std_[a-f0-9]{64}");
  private static final Set<String> MEDIA = Set.of("images", "documents", "videos");
  private static final Set<String> ASSET_FILTERS =
      Set.of("limit", "cursor", "kind", "media_type", "watermark_id");

  private final HttpClient http;
  private final String key, base;
  private final Duration timeout;

  /**
   * Creates a client for {@value #DEFAULT_BASE_URL} with a two-minute deadline.
   *
   * @param apiKey Etchv API key
   * @throws IllegalArgumentException if the key is blank or contains line breaks
   */
  public EtchvClient(String apiKey) {
    this(apiKey, DEFAULT_BASE_URL, DEFAULT_TIMEOUT);
  }

  /**
   * Creates a client.
   *
   * @param apiKey Etchv API key
   * @param baseUrl API base URL; must use HTTPS (HTTP is allowed for localhost only)
   * @param timeout positive deadline for each method call, including polling and retries
   * @throws IllegalArgumentException if an argument is invalid
   */
  public EtchvClient(String apiKey, String baseUrl, Duration timeout) {
    if (apiKey == null
        || apiKey.isBlank()
        || apiKey.contains("\r")
        || apiKey.contains("\n")
        || timeout == null
        || timeout.isNegative()
        || timeout.isZero())
      throw new IllegalArgumentException("API key and positive timeout are required");
    URI u;
    try {
      u = URI.create(Objects.requireNonNull(baseUrl));
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Invalid base URL");
    }
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
    http =
        HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NEVER)
            .connectTimeout(timeout)
            .build();
  }

  /** Closes the HTTP connection pool. */
  @Override
  public void close() {
    http.close();
  }

  /** Returns a description without credentials. */
  @Override
  public String toString() {
    return "EtchvClient[baseUrl=" + base + ", timeout=" + timeout + "]";
  }

  // ---------------------------------------------------------------- Connection check

  /**
   * Checks the API key without consuming credits ({@code GET /auth/api-key}).
   *
   * @return organization, key ID and scopes of the configured key
   * @throws EtchvException 401 for a missing or invalid key, 403 for a key without scopes
   * @throws InterruptedException if the thread is interrupted
   */
  public ApiKeyInfo getApiKeyInfo() throws InterruptedException {
    return json("auth/api-key", "GET", null, ApiKeyInfo.class);
  }

  // ---------------------------------------------------------------- Synchronous media

  /**
   * Watermarks an image and waits for the verified result.
   *
   * <p>Generates an idempotency key when none is supplied, retries transient failures and polls
   * the durable job until the client deadline.
   *
   * @param file encoded image bytes (1 byte to 20 MB)
   * @param data non-empty forensic JSON object
   * @param options request options, or null
   * @return the watermarked file
   * @throws EtchvException on API, transport or deadline failure
   * @throws InterruptedException if the thread is interrupted
   */
  public EmbedResult embedImage(byte[] file, Map<String, ?> data, Options options)
      throws InterruptedException {
    return embed("images", file, data, options);
  }

  /**
   * Watermarks a PDF and waits for the verified result.
   *
   * @param file PDF bytes (1 byte to 20 MB)
   * @param data non-empty forensic JSON object
   * @param options request options, or null
   * @return the watermarked file
   * @throws EtchvException on API, transport or deadline failure
   * @throws InterruptedException if the thread is interrupted
   * @see #embedImage
   */
  public EmbedResult embedDocument(byte[] file, Map<String, ?> data, Options options)
      throws InterruptedException {
    return embed("documents", file, data, options);
  }

  /**
   * Watermarks a video and waits for the verified result.
   *
   * @param file MP4 or MOV bytes (1 byte to 20 MB)
   * @param data non-empty forensic JSON object
   * @param options request options, or null
   * @return the watermarked file
   * @throws EtchvException on API, transport or deadline failure
   * @throws InterruptedException if the thread is interrupted
   * @see #embedImage
   */
  public EmbedResult embedVideo(byte[] file, Map<String, ?> data, Options options)
      throws InterruptedException {
    return embed("videos", file, data, options);
  }

  /**
   * Detects a watermark in an image. Synchronous image detection is not retried automatically.
   *
   * @param file encoded image bytes (1 byte to 20 MB)
   * @param options request options (storage options are not allowed), or null
   * @return detection result
   * @throws EtchvException on API or transport failure
   * @throws InterruptedException if the thread is interrupted
   */
  public DetectionResult detectImage(byte[] file, Options options) throws InterruptedException {
    return detect("images", file, options);
  }

  /**
   * Detects watermarks in a PDF, page by page. Not retried automatically.
   *
   * @param file PDF bytes (1 byte to 20 MB)
   * @param options request options (storage options are not allowed), or null
   * @return detection result
   * @throws EtchvException on API or transport failure
   * @throws InterruptedException if the thread is interrupted
   */
  public DetectionResult detectDocument(byte[] file, Options options) throws InterruptedException {
    return detect("documents", file, options);
  }

  /**
   * Detects watermarks in a video, frame by frame. Uses a durable job: generates an idempotency
   * key, retries transient failures and polls until the client deadline.
   *
   * @param file MP4 or MOV bytes (1 byte to 20 MB)
   * @param options request options (storage options are not allowed), or null
   * @return detection result
   * @throws EtchvException on API, transport or deadline failure
   * @throws InterruptedException if the thread is interrupted
   */
  public DetectionResult detectVideo(byte[] file, Options options) throws InterruptedException {
    return detect("videos", file, options);
  }

  // ---------------------------------------------------------------- Async jobs

  /**
   * Submits a background watermarking job ({@code POST /watermarks/{media}/async}) and returns
   * its job receipt without waiting for processing.
   *
   * @param media {@code images}, {@code documents} or {@code videos}
   * @param file file bytes (1 byte to 20 MB)
   * @param data non-empty forensic JSON object
   * @param options request options, or null; an idempotency key is generated when absent, but
   *     persist your own to recover a lost receipt
   * @param webhookId optional webhook endpoint ID ({@code wh_…}), or null
   * @return JSON job receipt with {@code request_id}, {@code status}, {@code status_url} and
   *     {@code result_url}
   * @throws EtchvException on API, transport or deadline failure
   * @throws InterruptedException if the thread is interrupted
   */
  public JsonObject submitEmbed(
      String media, byte[] file, Map<String, ?> data, Options options, String webhookId)
      throws InterruptedException {
    if (data == null || data.isEmpty())
      throw new IllegalArgumentException("data must be a non-empty JSON object");
    return submit(media, file, DATA_JSON.toJson(data), options, webhookId);
  }

  /**
   * Submits a background detection job ({@code POST /watermarks/{media}/detect/async}).
   *
   * @param media {@code images}, {@code documents} or {@code videos}
   * @param file file bytes (1 byte to 20 MB)
   * @param options request options (storage options are not allowed), or null
   * @param webhookId optional webhook endpoint ID ({@code wh_…}), or null
   * @return JSON job receipt
   * @throws EtchvException on API, transport or deadline failure
   * @throws InterruptedException if the thread is interrupted
   */
  public JsonObject submitDetection(String media, byte[] file, Options options, String webhookId)
      throws InterruptedException {
    return submit(media, file, null, options, webhookId);
  }

  /**
   * Reads a job receipt ({@code GET /watermarks/jobs/{id}} or {@code
   * /watermarks/detection-jobs/{id}}).
   *
   * @param requestId job ID ({@code req_…})
   * @param detect true for a detection job, false for an embedding job
   * @return JSON job receipt; {@code status} is {@code queued}, {@code running}, {@code
   *     retrying}, {@code succeeded} or {@code failed}
   * @throws EtchvException on API or transport failure
   * @throws InterruptedException if the thread is interrupted
   */
  public JsonObject getJob(String requestId, boolean detect) throws InterruptedException {
    if (!matches(JOB_ID, requestId)) throw new IllegalArgumentException("Invalid request ID");
    var r =
        request(
            "watermarks/" + (detect ? "detection-jobs" : "jobs") + "/" + requestId,
            null,
            null,
            new Options(),
            false,
            detect);
    return parseObject(r.body());
  }

  /**
   * Retrieves a saved embedding result ({@code GET /watermarks/jobs/{id}/result}), polling while
   * the job returns HTTP 202 until it finishes or the client deadline passes.
   *
   * @param requestId job ID ({@code req_…})
   * @return the watermarked file
   * @throws EtchvException 410 when the result expired or its asset was deleted ({@link
   *     EtchvException#isGone()}), 422/503 for a failed job, status 0 at the client deadline
   * @throws InterruptedException if the thread is interrupted
   */
  public EmbedResult getEmbedResult(String requestId) throws InterruptedException {
    if (!matches(JOB_ID, requestId)) throw new IllegalArgumentException("Invalid request ID");
    return embedding(
        request("watermarks/jobs/" + requestId + "/result", null, null, new Options(), true, false));
  }

  /**
   * Retrieves a detection job result ({@code GET /watermarks/detection-jobs/{id}/result}),
   * polling while the job returns HTTP 202.
   *
   * @param requestId job ID ({@code req_…})
   * @return detection result
   * @throws EtchvException 410 when the result expired, 503 for a failed job, status 0 at the
   *     client deadline
   * @throws InterruptedException if the thread is interrupted
   */
  public DetectionResult getDetectionResult(String requestId) throws InterruptedException {
    if (!matches(JOB_ID, requestId)) throw new IllegalArgumentException("Invalid request ID");
    return detection(
        request(
            "watermarks/detection-jobs/" + requestId + "/result",
            null,
            null,
            new Options(),
            true,
            true));
  }

  // ---------------------------------------------------------------- Assets

  /**
   * Lists assets ({@code GET /assets}), newest first.
   *
   * @param options filters: {@code limit}, {@code cursor}, {@code kind}, {@code media_type},
   *     {@code watermark_id}; null or empty for none
   * @return one page of assets
   * @throws EtchvException on API or transport failure
   * @throws InterruptedException if the thread is interrupted
   */
  public AssetPage listAssets(Map<String, String> options) throws InterruptedException {
    var query = new StringBuilder("assets");
    char separator = '?';
    if (options != null)
      for (var entry : new TreeMap<>(options).entrySet()) {
        if (!ASSET_FILTERS.contains(entry.getKey()) || entry.getValue() == null)
          throw new IllegalArgumentException("Unknown asset filter");
        query.append(separator).append(entry.getKey()).append('=').append(encode(entry.getValue()));
        separator = '&';
      }
    return json(query.toString(), "GET", null, AssetPage.class);
  }

  /**
   * Reads an asset record ({@code GET /assets/{id}}).
   *
   * @param id asset ID ({@code ast_…})
   * @return asset record including metadata
   * @throws EtchvException 404 for an unknown or deleted asset
   * @throws InterruptedException if the thread is interrupted
   */
  public Asset getAsset(String id) throws InterruptedException {
    return json(assetPath(id), "GET", null, Asset.class);
  }

  /**
   * Reads an asset record, optionally omitting metadata contents.
   *
   * @param id asset ID ({@code ast_…})
   * @param includeMetadata false to omit metadata
   * @return asset record
   * @throws EtchvException 404 for an unknown or deleted asset
   * @throws InterruptedException if the thread is interrupted
   */
  public Asset getAsset(String id, boolean includeMetadata) throws InterruptedException {
    return json(
        assetPath(id) + (includeMetadata ? "" : "?include_metadata=false"), "GET", null, Asset.class);
  }

  /**
   * Renames an asset or replaces its metadata ({@code PATCH /assets/{id}}).
   *
   * @param id asset ID ({@code ast_…})
   * @param version current asset version
   * @param changes {@code name} and/or {@code metadata}
   * @return updated asset
   * @throws EtchvException 409 when the version is stale
   * @throws InterruptedException if the thread is interrupted
   */
  public Asset updateAsset(String id, int version, Map<String, ?> changes)
      throws InterruptedException {
    var body = new LinkedHashMap<String, Object>(changes);
    body.put("version", version);
    return json(assetPath(id), "PATCH", body, Asset.class);
  }

  /**
   * Deletes an asset ({@code DELETE /assets/{id}}).
   *
   * @param id asset ID ({@code ast_…})
   * @throws EtchvException on API or transport failure
   * @throws InterruptedException if the thread is interrupted
   */
  public void deleteAsset(String id) throws InterruptedException {
    send(assetPath(id), "DELETE", null);
  }

  /**
   * Deletes 1–50 assets atomically ({@code POST /assets/bulk-delete}).
   *
   * @param ids asset IDs
   * @throws EtchvException on API or transport failure
   * @throws InterruptedException if the thread is interrupted
   */
  public void deleteAssets(List<String> ids) throws InterruptedException {
    if (ids == null || ids.isEmpty() || ids.size() > 50)
      throw new IllegalArgumentException("Provide 1–50 asset IDs");
    ids.forEach(EtchvClient::assetPath);
    send("assets/bulk-delete", "POST", Map.of("asset_ids", ids));
  }

  /**
   * Downloads an asset file ({@code GET /assets/{id}/content}).
   *
   * @param id asset ID ({@code ast_…})
   * @return file bytes in the original format
   * @throws EtchvException 410 when the file is no longer available
   * @throws InterruptedException if the thread is interrupted
   */
  public byte[] downloadAsset(String id) throws InterruptedException {
    return send(assetPath(id) + "/content", "GET", null).body();
  }

  // ---------------------------------------------------------------- Webhooks

  /**
   * Lists webhook endpoints ({@code GET /webhooks}). Requires {@code webhooks:read}.
   *
   * @return endpoints, newest first
   * @throws EtchvException on API or transport failure
   * @throws InterruptedException if the thread is interrupted
   */
  public List<WebhookEndpoint> listWebhooks() throws InterruptedException {
    return List.copyOf(
        JSON.<List<WebhookEndpoint>>fromJson(
            text(send("webhooks", "GET", null).body()),
            TypeToken.getParameterized(List.class, WebhookEndpoint.class).getType()));
  }

  /**
   * Creates a webhook endpoint ({@code POST /webhooks}). Requires {@code webhooks:write} and
   * owner or admin membership.
   *
   * @param url public HTTPS URL on port 443
   * @return the endpoint including its one-time {@code signingSecret}; store it securely
   * @throws EtchvException 409 at the 10-endpoint limit, 422 for an unsupported URL
   * @throws InterruptedException if the thread is interrupted
   */
  public WebhookEndpoint createWebhook(String url) throws InterruptedException {
    if (url == null || url.isBlank()) throw new IllegalArgumentException("Webhook URL is required");
    return json("webhooks", "POST", Map.of("url", url), WebhookEndpoint.class);
  }

  /**
   * Enables or disables a webhook endpoint ({@code PATCH /webhooks/{id}}).
   *
   * @param id endpoint ID ({@code wh_…})
   * @param enabled whether deliveries are sent
   * @return updated endpoint
   * @throws EtchvException on API or transport failure
   * @throws InterruptedException if the thread is interrupted
   */
  public WebhookEndpoint updateWebhook(String id, boolean enabled) throws InterruptedException {
    return json(webhookPath(id), "PATCH", Map.of("enabled", enabled), WebhookEndpoint.class);
  }

  /**
   * Permanently deletes a webhook endpoint ({@code DELETE /webhooks/{id}}).
   *
   * @param id endpoint ID ({@code wh_…})
   * @throws EtchvException on API or transport failure
   * @throws InterruptedException if the thread is interrupted
   */
  public void deleteWebhook(String id) throws InterruptedException {
    send(webhookPath(id), "DELETE", null);
  }

  /**
   * Lists an endpoint's deliveries from the last 30 days ({@code GET /webhooks/{id}/deliveries}).
   *
   * @param id endpoint ID ({@code wh_…})
   * @param after {@code nextCursor} from the previous page, or null for the first page
   * @return one page of up to 50 deliveries
   * @throws EtchvException on API or transport failure
   * @throws InterruptedException if the thread is interrupted
   */
  public WebhookDeliveryPage listWebhookDeliveries(String id, String after)
      throws InterruptedException {
    if (after != null && !matches(EVENT_ID, after))
      throw new IllegalArgumentException("Invalid delivery cursor");
    return json(
        webhookPath(id) + "/deliveries" + (after == null ? "" : "?after=" + after),
        "GET",
        null,
        WebhookDeliveryPage.class);
  }

  /**
   * Queues a manual redelivery ({@code POST /webhooks/{id}/deliveries/{eventId}/redeliver}).
   *
   * @param id endpoint ID ({@code wh_…})
   * @param eventId event ID ({@code evt_…})
   * @return JSON receipt with {@code id} and {@code status}
   * @throws EtchvException 409 when the delivery is pending, expired or at its limit
   * @throws InterruptedException if the thread is interrupted
   */
  public JsonObject redeliverWebhook(String id, String eventId) throws InterruptedException {
    if (!matches(EVENT_ID, eventId)) throw new IllegalArgumentException("Invalid event ID");
    return parseObject(
        send(webhookPath(id) + "/deliveries/" + eventId + "/redeliver", "POST", null).body());
  }

  /**
   * Verifies an Etchv webhook signature with the default five-minute tolerance. Verify the raw
   * request bytes before parsing JSON, then check that the body {@code id} matches {@code
   * X-Etchv-Event-ID}.
   *
   * @param rawBody raw request body bytes
   * @param timestamp {@code X-Etchv-Timestamp} header value
   * @param signature {@code X-Etchv-Signature} header value ({@code v1=…})
   * @param signingSecret the endpoint's full signing secret, including {@code whsec_}
   * @return true when the signature is valid and the timestamp is within tolerance
   */
  public static boolean verifyWebhookSignature(
      byte[] rawBody, String timestamp, String signature, String signingSecret) {
    return verifyWebhookSignature(
        rawBody, timestamp, signature, signingSecret, Instant.now(), WEBHOOK_TOLERANCE);
  }

  /**
   * Verifies an Etchv webhook signature against an explicit clock and tolerance.
   *
   * @param rawBody raw request body bytes
   * @param timestamp {@code X-Etchv-Timestamp} header value (Unix seconds)
   * @param signature {@code X-Etchv-Signature} header value ({@code v1=…})
   * @param signingSecret the endpoint's full signing secret, including {@code whsec_}
   * @param now current time
   * @param tolerance maximum difference between {@code now} and the timestamp
   * @return true when the signature is valid and the timestamp is within tolerance
   */
  public static boolean verifyWebhookSignature(
      byte[] rawBody,
      String timestamp,
      String signature,
      String signingSecret,
      Instant now,
      Duration tolerance) {
    if (rawBody == null
        || timestamp == null
        || signature == null
        || signingSecret == null
        || signingSecret.isEmpty()
        || now == null
        || tolerance == null
        || !timestamp.matches("[0-9]{1,12}")) return false;
    if (Math.abs(now.getEpochSecond() - Long.parseLong(timestamp)) > tolerance.toSeconds())
      return false;
    try {
      var mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(signingSecret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
      mac.update((timestamp + ".").getBytes(StandardCharsets.UTF_8));
      String expected = "v1=" + HexFormat.of().formatHex(mac.doFinal(rawBody));
      return MessageDigest.isEqual(
          expected.getBytes(StandardCharsets.UTF_8), signature.getBytes(StandardCharsets.UTF_8));
    } catch (GeneralSecurityException e) {
      return false;
    }
  }

  // ---------------------------------------------------------------- Storage

  /**
   * Lists customer storage destinations ({@code GET /storage/destinations}). Requires {@code
   * storage:read}.
   *
   * @return destination records as JSON objects (credentials are never returned)
   * @throws EtchvException on API or transport failure
   * @throws InterruptedException if the thread is interrupted
   */
  public List<JsonObject> listStorageDestinations() throws InterruptedException {
    var list = new ArrayList<JsonObject>();
    for (var item :
        JsonParser.parseString(text(send("storage/destinations", "GET", null).body()))
            .getAsJsonArray()) list.add(item.getAsJsonObject());
    return List.copyOf(list);
  }

  /**
   * Creates a storage destination ({@code POST /storage/destinations}). Requires {@code
   * storage:write} and owner or admin membership. Verify it before use.
   *
   * @param destination fields such as {@code name}, {@code provider} ({@code s3}, {@code gcs},
   *     {@code azure}), {@code bucket}, {@code prefix}, {@code visibility}, {@code region},
   *     {@code role_arn}, {@code account}, {@code credentials} and GCS federation fields; see the
   *     storage guide
   * @return created destination (without credentials)
   * @throws EtchvException 409 at the 10-destination limit, 422 for invalid fields
   * @throws InterruptedException if the thread is interrupted
   */
  public JsonObject createStorageDestination(Map<String, ?> destination)
      throws InterruptedException {
    if (destination == null || destination.isEmpty())
      throw new IllegalArgumentException("Destination fields are required");
    return parseObject(send("storage/destinations", "POST", destination).body());
  }

  /**
   * Enables, disables or replaces credentials of a destination ({@code PATCH
   * /storage/destinations/{id}}). New credentials require verification again.
   *
   * @param id destination ID ({@code dst_…})
   * @param changes {@code enabled} and/or {@code credentials}
   * @return updated destination
   * @throws EtchvException 409 when the destination changed concurrently
   * @throws InterruptedException if the thread is interrupted
   */
  public JsonObject updateStorageDestination(String id, Map<String, ?> changes)
      throws InterruptedException {
    if (changes == null || changes.isEmpty())
      throw new IllegalArgumentException("Destination changes are required");
    return parseObject(send(destinationPath(id), "PATCH", changes).body());
  }

  /**
   * Deletes a storage destination ({@code DELETE /storage/destinations/{id}}).
   *
   * @param id destination ID ({@code dst_…})
   * @throws EtchvException on API or transport failure
   * @throws InterruptedException if the thread is interrupted
   */
  public void deleteStorageDestination(String id) throws InterruptedException {
    send(destinationPath(id), "DELETE", null);
  }

  /**
   * Writes and reads a test object to verify a destination ({@code POST
   * /storage/destinations/{id}/verify}).
   *
   * @param id destination ID ({@code dst_…})
   * @return verified destination
   * @throws EtchvException 422 when the connection check fails
   * @throws InterruptedException if the thread is interrupted
   */
  public JsonObject verifyStorageDestination(String id) throws InterruptedException {
    return parseObject(send(destinationPath(id) + "/verify", "POST", null).body());
  }

  /**
   * Lists deliveries to a destination ({@code GET /storage/destinations/{id}/deliveries}).
   *
   * @param id destination ID ({@code dst_…})
   * @param after {@code next_cursor} from the previous page, or null
   * @return JSON page with {@code items} and {@code next_cursor}
   * @throws EtchvException on API or transport failure
   * @throws InterruptedException if the thread is interrupted
   */
  public JsonObject listStorageDeliveries(String id, String after) throws InterruptedException {
    if (after != null && !matches(DELIVERY_ID, after))
      throw new IllegalArgumentException("Invalid delivery cursor");
    return parseObject(
        send(
                destinationPath(id) + "/deliveries" + (after == null ? "" : "?after=" + after),
                "GET",
                null)
            .body());
  }

  /**
   * Copies an existing watermarked asset to a destination ({@code POST
   * /storage/destinations/{id}/deliveries}).
   *
   * @param id verified destination ID ({@code dst_…})
   * @param assetId watermarked asset ID ({@code ast_…})
   * @param key optional relative object key beneath the destination prefix, or null
   * @return queued delivery record
   * @throws EtchvException 404 when no available watermarked asset exists, 409 on key conflict
   * @throws InterruptedException if the thread is interrupted
   */
  public JsonObject createStorageDelivery(String id, String assetId, String key)
      throws InterruptedException {
    assetPath(assetId);
    var body = new LinkedHashMap<String, Object>();
    body.put("asset_id", assetId);
    if (key != null) body.put("key", key);
    return parseObject(send(destinationPath(id) + "/deliveries", "POST", body).body());
  }

  /**
   * Reads a storage delivery ({@code GET /storage/deliveries/{id}}). Returns 404 until the
   * watermark job has succeeded.
   *
   * @param id delivery ID ({@code std_…})
   * @return delivery record; poll until {@code status} is {@code stored} or terminal
   * @throws EtchvException on API or transport failure
   * @throws InterruptedException if the thread is interrupted
   */
  public JsonObject getStorageDelivery(String id) throws InterruptedException {
    return parseObject(send(deliveryPath(id), "GET", null).body());
  }

  /**
   * Retries a failed or cancelled storage delivery ({@code POST /storage/deliveries/{id}/retry}).
   * Does not watermark again or consume credits.
   *
   * @param id delivery ID ({@code std_…})
   * @return queued delivery record
   * @throws EtchvException 409 when the delivery is pending, stored, expired or at its limit
   * @throws InterruptedException if the thread is interrupted
   */
  public JsonObject retryStorageDelivery(String id) throws InterruptedException {
    return parseObject(send(deliveryPath(id) + "/retry", "POST", null).body());
  }

  /**
   * Downloads a stored delivery from the customer bucket ({@code GET
   * /storage/deliveries/{id}/content}).
   *
   * @param id delivery ID ({@code std_…})
   * @return file bytes
   * @throws EtchvException 409 when the delivery is not stored yet
   * @throws InterruptedException if the thread is interrupted
   */
  public byte[] downloadStorageDelivery(String id) throws InterruptedException {
    return send(deliveryPath(id) + "/content", "GET", null).body();
  }

  // ---------------------------------------------------------------- Internals

  private static boolean matches(Pattern pattern, String value) {
    return value != null && pattern.matcher(value).matches();
  }

  private static String assetPath(String id) {
    if (!matches(ASSET_ID, id)) throw new IllegalArgumentException("Invalid asset ID");
    return "assets/" + id;
  }

  private static String webhookPath(String id) {
    if (!matches(WEBHOOK_ID, id)) throw new IllegalArgumentException("Invalid webhook ID");
    return "webhooks/" + id;
  }

  private static String destinationPath(String id) {
    if (!matches(DESTINATION_ID, id))
      throw new IllegalArgumentException("Invalid storage destination ID");
    return "storage/destinations/" + id;
  }

  private static String deliveryPath(String id) {
    if (!matches(DELIVERY_ID, id))
      throw new IllegalArgumentException("Invalid storage delivery ID");
    return "storage/deliveries/" + id;
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  private static String text(byte[] body) {
    return new String(body, StandardCharsets.UTF_8);
  }

  private static String bounded(byte[] body) {
    return new String(body, 0, Math.min(10000, body.length), StandardCharsets.UTF_8);
  }

  private static JsonObject parseObject(byte[] body) {
    try {
      return JsonParser.parseString(text(body)).getAsJsonObject();
    } catch (RuntimeException e) {
      throw new EtchvException(200, "Invalid JSON response", null, null);
    }
  }

  private HttpRequest.Builder newRequest(String path, Duration requestTimeout) {
    return HttpRequest.newBuilder(URI.create(base + "/" + path))
        .header("X-API-Key", key)
        .header("User-Agent", USER_AGENT)
        .timeout(requestTimeout);
  }

  /** Single JSON/management request without automatic retries; accepts any 2xx status. */
  private HttpResponse<byte[]> send(String path, String method, Object body)
      throws InterruptedException {
    var builder = newRequest(path, timeout);
    if (body != null) builder.header("Content-Type", "application/json");
    builder.method(
        method,
        body == null
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString(JSON.toJson(body)));
    HttpResponse<byte[]> response;
    try {
      response = http.send(builder.build(), ignored -> new LimitedBody());
    } catch (IOException e) {
      throw new EtchvException(0, e.getMessage(), null, null, e);
    }
    int status = response.statusCode();
    if (status < 200 || status > 299)
      throw new EtchvException(
          status,
          bounded(response.body()),
          response.headers().firstValue("X-Request-ID").orElse(null),
          null);
    return response;
  }

  private <T> T json(String path, String method, Object body, Class<T> type)
      throws InterruptedException {
    var response = send(path, method, body);
    try {
      T value = JSON.fromJson(text(response.body()), type);
      if (value == null) throw new JsonParseException("empty");
      return value;
    } catch (RuntimeException e) {
      throw new EtchvException(
          response.statusCode(),
          "Invalid JSON response",
          response.headers().firstValue("X-Request-ID").orElse(null),
          null);
    }
  }

  private JsonObject submit(
      String media, byte[] file, String data, Options options, String webhookId)
      throws InterruptedException {
    if (!MEDIA.contains(media)
        || file == null
        || file.length == 0
        || file.length > MAX_FILE_SIZE)
      throw new IllegalArgumentException("Invalid media or file size");
    if (webhookId != null && !matches(WEBHOOK_ID, webhookId))
      throw new IllegalArgumentException("Invalid webhook ID");
    if (options == null) options = new Options();
    String idempotency = options.idempotencyKey();
    if (idempotency == null || idempotency.isEmpty()) idempotency = UUID.randomUUID().toString();
    var r =
        request(
            "watermarks/"
                + media
                + (data == null ? "/detect" : "")
                + "/async"
                + (webhookId == null ? "" : "?webhook_id=" + webhookId),
            file,
            data,
            new Options(
                options.filename() == null ? defaultFilename(media) : options.filename(),
                idempotency,
                options.storageDestinationId(),
                options.storageKey()),
            true,
            data == null);
    return parseObject(r.body());
  }

  private static String defaultFilename(String media) {
    return switch (media) {
      case "documents" -> "document.pdf";
      case "videos" -> "video.mp4";
      default -> "image.png";
    };
  }

  private EmbedResult embed(String media, byte[] file, Map<String, ?> data, Options options)
      throws InterruptedException {
    if (data == null || data.isEmpty())
      throw new IllegalArgumentException("data must be a non-empty JSON object");
    return embedding(post(media, file, DATA_JSON.toJson(data), options));
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
    String filename = options.filename() != null ? options.filename() : defaultFilename(media);
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

  /** Media request with optional durable retries and 202 job continuation. */
  private HttpResponse<byte[]> request(
      String path, byte[] file, String data, Options options, boolean durable, boolean detectionJob)
      throws InterruptedException {
    if (options.storageKey() != null && options.storageDestinationId() == null)
      throw new IllegalArgumentException("Storage key requires destination");
    if (options.storageDestinationId() != null) {
      if (data == null || !matches(DESTINATION_ID, options.storageDestinationId()))
        throw new IllegalArgumentException("Invalid storage destination or detection request");
      path +=
          (path.contains("?") ? "&" : "?")
              + "storage_destination_id="
              + encode(options.storageDestinationId());
      if (options.storageKey() != null) path += "&storage_key=" + encode(options.storageKey());
    }
    long started = System.nanoTime();
    String requestId = null;
    String boundary = "etchv-" + UUID.randomUUID();
    byte[] body = file == null ? null : multipart(file, data, options.filename(), boundary);
    IOException lastFailure = null;
    while (System.nanoTime() - started < timeout.toNanos()) {
      if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
      var builder =
          newRequest(
              path,
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
          throw new EtchvException(0, e.getMessage(), requestId, options.idempotencyKey(), e);
        lastFailure = e;
        pause(started, 1);
        continue;
      }
      requestId = response.headers().firstValue("X-Request-ID").orElse(requestId);
      int status = response.statusCode();
      if (status == 200 || (status == 202 && path.split("\\?", 2)[0].endsWith("/async")))
        return response;
      JsonObject detail = new JsonObject();
      try {
        var value = JsonParser.parseString(text(response.body()));
        if (value.isJsonObject()) detail = value.getAsJsonObject();
      } catch (JsonParseException ignored) {
      }
      if (durable && status == 202) {
        var id = detail.get("request_id");
        if (id == null
            || !id.isJsonPrimitive()
            || !id.getAsJsonPrimitive().isString()
            || !matches(JOB_ID, id.getAsString()))
          throw new EtchvException(
              202, "Invalid job response", requestId, options.idempotencyKey());
        requestId = id.getAsString();
        // Never follow server-provided URLs; rebuild the path from the validated job ID.
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
          status, bounded(response.body()), requestId, options.idempotencyKey());
    }
    throw new EtchvException(
        0,
        "Client deadline exceeded; job may still complete",
        requestId,
        options.idempotencyKey(),
        lastFailure);
  }

  private void pause(long started, double seconds) throws InterruptedException {
    long remaining = timeout.toNanos() - (System.nanoTime() - started);
    if (remaining > 0) TimeUnit.NANOSECONDS.sleep(Math.min(remaining, (long) (seconds * 1e9)));
  }

  private static EmbedResult embedding(HttpResponse<byte[]> r) {
    String mime = r.headers().firstValue("Content-Type").orElse("").split(";")[0].trim(),
        id = r.headers().firstValue("X-Watermark-ID").orElse(null),
        requestId = r.headers().firstValue("X-Request-ID").orElse(null);
    String ext = extension(r.body(), mime);
    if (ext == null || !matches(WATERMARK_ID, id))
      throw new EtchvException(200, "Invalid embedding response", requestId, null);
    var match =
        Pattern.compile("filename=\"([A-Za-z0-9._-]+)\"")
            .matcher(r.headers().firstValue("Content-Disposition").orElse(""));
    return new EmbedResult(
        r.body(),
        id,
        requestId,
        mime,
        match.find() ? match.group(1) : "watermarked." + ext,
        r.headers().firstValue("X-Asset-ID").orElse(null),
        r.headers().firstValue("X-Source-Asset-ID").orElse(null),
        r.headers().firstValue("X-Storage-Delivery-ID").orElse(null));
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
                || !matches(WATERMARK_ID, id.getAsString())
            : !id.isJsonNull())) throw new IllegalArgumentException();
    return new DetectionUnit(index, marked, confidence, marked ? id.getAsString() : null);
  }

  private static DetectionResult detection(HttpResponse<byte[]> r) {
    String requestId = r.headers().firstValue("X-Request-ID").orElse(null);
    try {
      var v = JsonParser.parseString(text(r.body())).getAsJsonObject();
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
      case "image/jpeg" -> s.startsWith("ÿØÿ") ? "jpg" : null;
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
