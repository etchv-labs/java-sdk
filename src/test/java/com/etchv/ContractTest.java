package com.etchv;

import com.google.gson.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;

public final class ContractTest {
  static void check(boolean ok) {
    if (!ok) throw new AssertionError("Contract assertion failed");
  }

  @org.junit.jupiter.api.Test
  void allFormatsAndDurableProtocol() throws Exception {
    var formats =
        JsonParser.parseString(Files.readString(Path.of("tests/formats.json"))).getAsJsonArray();
    try (var server = new FixtureServer(formats)) {
      String base = server.base;
      var data = Map.of("asset", "example");
      for (var value : formats) {
        var f = value.getAsJsonObject();
        String ext = f.get("extension").getAsString();
        byte[] bytes = Base64.getDecoder().decode(f.get("base64").getAsString());
        var options = new EtchvClient.Options("input." + ext, null);
        try (var c =
            new EtchvClient("test-key", base + "/formats/" + ext, Duration.ofSeconds(10))) {
          var result =
              switch (f.get("media").getAsString()) {
                case "documents" -> c.embedDocument(bytes, data, options);
                case "videos" -> c.embedVideo(bytes, data, options);
                default -> c.embedImage(bytes, data, options);
              };
          check(
              Arrays.equals(result.bytes(), bytes)
                  && result.contentType().equals(f.get("mime").getAsString())
                  && result.filename().equals("protected." + ext));
          var d =
              switch (f.get("media").getAsString()) {
                case "documents" -> c.detectDocument(bytes, options);
                case "videos" -> c.detectVideo(bytes, options);
                default -> c.detectImage(bytes, options);
              };
          check(d.watermarked() && d.units().size() == 2);
        }
      }
      for (String scenario :
          List.of(
              "retry",
              "embed-job",
              "detect-job",
              "failed",
              "redirect",
              "invalid",
              "bad-job",
              "bad-id",
              "bad-detection",
              "bad-units",
              "deadline")) {
        String ext =
            switch (scenario) {
              case "embed-job" -> "pdf";
              case "detect-job" -> "mp4";
              default -> "png";
            };
        byte[] bytes = null;
        for (var f : formats)
          if (f.getAsJsonObject().get("extension").getAsString().equals(ext))
            bytes = Base64.getDecoder().decode(f.getAsJsonObject().get("base64").getAsString());
        try (var c =
            new EtchvClient(
                "test-key",
                base + "/" + scenario + "/" + ext,
                Duration.ofMillis(scenario.equals("deadline") ? 150 : 10000))) {
          var opts = new EtchvClient.Options("input." + ext, "stable-key");
          EtchvClient.EtchvException error = null;
          try {
            switch (scenario) {
              case "embed-job" -> c.embedDocument(bytes, data, opts);
              case "detect-job" -> c.detectVideo(bytes, opts);
              case "bad-detection", "bad-units" -> c.detectImage(bytes, opts);
              default -> c.embedImage(bytes, data, opts);
            }
          } catch (EtchvClient.EtchvException e) {
            error = e;
          }
          if (List.of("retry", "embed-job", "detect-job").contains(scenario)) check(error == null);
          else {
            check(error != null);
            if (scenario.equals("deadline"))
              check(
                  error.statusCode == 0
                      && "stable-key".equals(error.idempotencyKey)
                      && error.requestId != null);
          }
        }
      }
      for (String url :
          List.of(
              "http://example.com", "https://user:pass@example.com", "https://example.com?x=1")) {
        boolean rejected = false;
        try (var c = new EtchvClient("key", url, Duration.ofSeconds(1))) {
        } catch (IllegalArgumentException e) {
          rejected = true;
        }
        check(rejected);
      }
      try (var c = new EtchvClient("key")) {
        boolean rejected = false;
        try {
          c.getEmbedResult("../../steal");
        } catch (IllegalArgumentException e) {
          rejected = true;
        }
        check(rejected);
      }
      server.verify();
    }
  }
}
