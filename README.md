# Etchv Java SDK

Server-side Java client for [Etchv](https://etchv.com): embed and detect invisible forensic watermarks in images, PDFs and videos.

## Install

Maven:

```xml
<dependency><groupId>com.etchv</groupId><artifactId>etchv-sdk</artifactId><version>1.6.0</version></dependency>
```

Gradle:

```kotlin
implementation("com.etchv:etchv-sdk:1.6.0")
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

Image and PDF uploads are limited to 50 MB; DOCX, PPTX and video uploads to 20 MB. A PDF has 1 to 2,000
pages, and its watermarked copy must fit in 95 MB with 48 KB per page, so a 2,000-page PDF can be up to about
1 MB. Detection takes the files Etchv delivered: up to 192 MB for images, 95 MB for PDFs and 100 MB for DOCX,
PPTX and video.

## Large files

Files over 40 MB are uploaded once to a signed URL and then referenced by ID, so the request never
carries the file. This is automatic in every embed, detect and submit method; retries reuse the same
upload. Image and document detection above 95 MB runs as a background job and the call waits for it.
Change the threshold with `new EtchvClient(apiKey, baseUrl, timeout, largeFileThreshold)`, or upload
explicitly:

```java
var upload = client.uploadFile("detect", delivered, "delivered.tiff");
upload.uploadId(); // send as the upload_id form field instead of file
```

## Many files at once

Submit up to 100 files in one batch, each with its own data, then wait and collect the results:

```java
var items = new ArrayList<EtchvClient.BatchItem>();
try (var files = Files.newDirectoryStream(Path.of("in"), "*.pdf")) {
    for (var file : files) {
        String stem = file.getFileName().toString().replaceFirst("\\.pdf$", "");
        items.add(EtchvClient.BatchItem.of(file, Map.of("recipient", stem)));
    }
}
var batch = client.submitBatch(items, new EtchvClient.BatchOptions().withArchive(true));

// Waits for the batch (here up to 30 minutes), then downloads each result as you iterate.
try (var results = client.batchResults(batch.batchId(), Duration.ofMinutes(30))) {
    for (var item : (Iterable<EtchvClient.BatchItemResult>) results::iterator) {
        if (item.ok()) Files.write(Path.of("out", item.filename()), item.result().bytes());
        else System.out.println(item.filename() + ": " + item.errorCode()); // credits refunded
    }
}
client.downloadBatchArchive(batch.batchId(), Path.of("out.zip"), Duration.ofMinutes(10));
```

`submitBatch` creates the batch, uploads every file straight to its own signed upload URL (four at a time by
default, `withUploadConcurrency`; the API key is never sent there) and starts it. A slow upload keeps going as long
as bytes flow. The batch's idempotency key is generated unless you set `withIdempotencyKey`, so submitting the same
files again with the same key is safe and resumes an interrupted upload; a batch that was never started within 24
hours has expired (HTTP 410, `code()` `batch_expired`) and needs a new key. Files that are rejected or fail are refunded; check each result's
`errorCode()` (`cancelled` or `expired` for files a canceled or expired batch never ran). Upload URLs last 6 hours
and a batch must start within 24 hours. Results and the optional archive (`withArchive`, up to 1 GB of results)
stay available for 24 hours. `waitForBatch(id, timeout)` and `batchResults(id, timeout)` honor the API's
`Retry-After` between polls, at least 1 s apart (the wait is one hour when the timeout is null).

`downloadBatchArchive` waits while the zip is assembled, then writes it to a `Path` or an `OutputStream` (or returns
the bytes); it fails only if no bytes arrive for the client timeout. A 409 carries `code()` `batch_not_started`,
`archive_not_requested`, `archive_too_large` or `archive_unavailable`.

Files that are already together in one zip (up to 55 MB) can go in a single request with `submitBatchZip`, naming
every member in its manifest:

```java
var batch = client.submitBatchZip(Path.of("contracts.zip"),
    List.of(new EtchvClient.BatchZipItem("contracts/acme.pdf", Map.of("recipient", "acme"))), null);
```

`BatchOptions` also takes `withWebhookId`, `withAccelerator`, `withStorageDestinationId` and `withActiveContent`, as
for single files. `getBatch`, `cancelBatch` (files not yet running are refunded) and `listBatches` complete the set.

## PDF scripts and actions

A watermarked PDF keeps its scripts, actions, form fields and attachments exactly as uploaded. Set
`ActiveContent.REMOVE_AUTO_RUN` with `Options.withActiveContent` (for `embedDocument` or `submitEmbed("documents",
…)`) or `BatchOptions.withActiveContent` to also remove what a PDF viewer runs by itself: the open action, document
and page actions, document-level JavaScript and XFA forms. Links, form fields and attachments stay.
`REMOVE_AUTO_RUN` is for PDFs only: the API refuses it for a DOCX or PPTX, and for a batch unless every file is a
PDF. Keeping active content is accepted for any document and any batch.
[Scripts and actions](https://etchv.com/docs/api/documents#scripts-and-actions).

```java
var result = client.embedDocument(Files.readAllBytes(Path.of("contract.pdf")), Map.of("recipient", "counsel-7"),
    new EtchvClient.Options("contract.pdf", null).withActiveContent(EtchvClient.ActiveContent.REMOVE_AUTO_RUN));
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

## Traceable links

Share one file so each recipient downloads their own watermarked copy. Use the source asset of a file you've
watermarked; send the returned `url()` yourself.

```java
var link = client.createLink(result.sourceAssetId(), "Board pack, October",
    EtchvClient.LinkOptions.recipients(List.of("alex@example.com", "sam@example.com")).withDownloadLimit(3));
System.out.println(link.url());
```

Use `LinkOptions.allowedDomains(List.of("example.com"))` instead of `recipients` to let anyone at those domains open
it; `withExpiresAt`, `withPrepare`, `withWebhookId` and `withIdempotencyKey` set the rest. Retries reuse one
idempotency key, so a retry returns the same link; a workspace without the credits for the copies gets HTTP 402 with
`code()` `insufficient_credits`. `estimateLink` takes the same arguments and reports the credits the copies would take
without creating anything. Also `getLink` (each recipient, their opens and their copy), `listLinks`, `listLinkEvents`
and `revokeLink`.

`deleteLink(id)` deletes a link with every recipient's opens, downloads and copy; `removeLinkRecipient(id, recipientId)`
does the same for one recipient (their `id()` in `getLink`) and returns the link without them. Neither can be undone,
and detection no longer names a deleted recipient. Both need an owner or admin and, for an API key, `assets:delete` as
well as `links:write`.

When a detected file is one of your links' copies, `DetectionResult.copy()` names the link, the recipient and when they
first opened it (needs `links:read`); `DetectionResult.asset()` names any other watermarked asset of yours (needs
`assets:read`). Both are null otherwise. See https://etchv.com/docs/api/links.

## Also included

- API key check: `getApiKeyInfo`
- Batches: `submitBatch`, `submitBatchZip`, `getBatch`, `waitForBatch`, `batchResults`, `downloadBatchArchive`, `cancelBatch`, `listBatches`
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
