# Etchv Java SDK

Server-side Java client for [Etchv](https://etchv.com): embed and detect invisible forensic watermarks in images, PDFs and videos.

## Install

Maven:

```xml
<dependency><groupId>com.etchv</groupId><artifactId>etchv-sdk</artifactId><version>1.0.0</version></dependency>
```

Gradle:

```kotlin
implementation("com.etchv:etchv-sdk:1.0.0")
```

Requires Java 21+. `EtchvClient` is thread-safe; create one and close it on shutdown.

## Quickstart

```java
import com.etchv.EtchvClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

class Example {
    public static void main(String[] args) throws Exception {
        try (var client = new EtchvClient(System.getenv("ETCHV_API_KEY"))) {
            var result = client.embedImage(Files.readAllBytes(Path.of("photo.jpg")),
                Map.of("recipient", "customer-123"), new EtchvClient.Options("photo.jpg", null));
            Files.write(Path.of(result.filename()), result.bytes());

            var detection = client.detectImage(result.bytes(), new EtchvClient.Options(result.filename(), null));
            System.out.println(detection.watermarked() + " " + detection.confidence());
        }
    }
}
```

## GPU processing

Business and Enterprise plans can request GPU processing for any embed, detect or async submission; other plans
receive HTTP 403. GPU operations cost 3× credits. If no GPU is ready, the request runs on CPU at normal credits.
`accelerator()` on the result reports the hardware that actually ran (null when not reported).

```java
var options = new EtchvClient.Options("photo.jpg", null).withAccelerator(EtchvClient.Accelerator.GPU);
var result = client.embedImage(bytes, Map.of("recipient", "customer-123"), options);
System.out.println(result.accelerator()); // gpu, or cpu after a fallback
```

Job receipts from `getJob` include `accelerator_requested` and `accelerator`.

## Async jobs

```java
var job = client.submitEmbed("documents", pdfBytes, Map.of("delivery", "delivery_001"),
    new EtchvClient.Options("report.pdf", "delivery_001"), null); // or a webhook ID
String requestId = job.get("request_id").getAsString();

var status = client.getJob(requestId, false).get("status").getAsString();
var result = client.getEmbedResult(requestId); // waits while the job is processing
```

Resending the same request with the same idempotency key returns the existing job without another charge.
Detection uses `submitDetection`, `getJob(requestId, true)` and `getDetectionResult`.

## Also included

- API key check: `getApiKeyInfo`
- Assets: `listAssets`, `getAsset`, `updateAsset`, `deleteAsset`, `deleteAssets`, `downloadAsset`
- Webhooks: `listWebhooks`, `createWebhook`, `updateWebhook`, `deleteWebhook`, `listWebhookDeliveries`, `redeliverWebhook`, and `EtchvClient.verifyWebhookSignature`
- Customer storage: `listStorageDestinations`, `createStorageDestination`, `updateStorageDestination`, `deleteStorageDestination`, `verifyStorageDestination`, `listStorageDeliveries`, `createStorageDelivery`, `getStorageDelivery`, `retryStorageDelivery`, `downloadStorageDelivery`

`verifyWebhookSignature` checks only the signature and timestamp; your handler must also compare the body `id` with `X-Etchv-Event-ID`.

## Errors

API and transport failures throw the unchecked `EtchvClient.EtchvException` with `statusCode()` (`0` when no HTTP
response), `requestId()`, `idempotencyKey()`, `detail()` and `isGone()` (HTTP 410). Include the request ID when
contacting support.

Durable operations (embedding, video detection, async submissions and job results) retry HTTP 429, 502, 503 and
504 with the same idempotency key until the client deadline; rate-limited responses wait for `Retry-After` (up to
5 s per wait). Structured errors include the API's message in `getMessage()` and expose `code()` (for example
`rate_limited` or `concurrency_limited`), `detailMessage()` and `limit()`; a 429 also carries `retryAfter()`.

```java
} catch (EtchvClient.EtchvException e) {
    System.err.println("HTTP " + e.statusCode() + ", request " + e.requestId());
}
```

## Links

- Full guide: https://etchv.com/docs/sdks/java
- API reference: https://etchv.com/docs
- Support: hello@etchv.com

License: MIT.

Questions or bug reports: open an issue here or email hello@etchv.com.
