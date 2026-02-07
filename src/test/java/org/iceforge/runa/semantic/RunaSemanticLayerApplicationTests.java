package org.iceforge.runa.semantic;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class RunaSemanticLayerApplicationTests {

    static MockWebServer skadi;

    @LocalServerPort
    int port;

    WebTestClient client;

    static {
        try {
            skadi = new MockWebServer();
            skadi.start();
        } catch (IOException e) {
            throw new RuntimeException("Failed to start MockWebServer", e);
        }
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("runa.skadiBaseUrl", () -> skadi.url("/").toString());
        r.add("runa.skadiSubmitPath", () -> "/api/v1/queries");
        r.add("runa.skadiArrowResultPathTemplate", () -> "/api/v1/queries/{queryId}/results/arrow");
        r.add("warehouse.jdbcUrl", () -> "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (skadi != null) skadi.shutdown();
    }

    @BeforeEach
    void setup() throws Exception {
        while (skadi.takeRequest(1, TimeUnit.MILLISECONDS) != null) {
            // drain
        }

        client = WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .responseTimeout(Duration.ofSeconds(10))
                .build();
    }

    @Test
    void proxiesArrowWhenSkadiReturnsArrowDirectly() throws Exception {
        byte[] fakeArrow = new byte[]{1, 2, 3, 4, 5};

        // If your endpoint first "submits" and then fetches Arrow, you must enqueue BOTH.
        skadi.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody("{\"queryId\":\"q-1\"}"));

        skadi.enqueue(new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/vnd.apache.arrow.stream")
                .setBody(new Buffer().write(fakeArrow)));

        var spec = client.post()
                .uri("/api/semantic/query")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.valueOf("application/vnd.apache.arrow.stream"))
                .bodyValue("""
                        {
                          "asOf":"2026-02-06",
                          "metrics":["var_99_10d_usd"],
                          "groupBy":["desk"]
                        }
                        """)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentType("application/vnd.apache.arrow.stream");

        // Prove the app hit Skadi twice (submit + arrow fetch).
        var submitReq = skadi.takeRequest(1, TimeUnit.SECONDS);
        var arrowReq = skadi.takeRequest(1, TimeUnit.SECONDS);

        assertThat(submitReq).as("Expected Skadi submit request").isNotNull();
        assertThat(arrowReq).as("Expected Skadi arrow request").isNotNull();
        assertThat(submitReq.getPath()).contains("/api/v1/queries");
        assertThat(arrowReq.getPath()).contains("/results/arrow");

        byte[] resp = readAllBytesOrFail(spec, dumpRecordedRequests(submitReq, arrowReq))
                .block(Duration.ofSeconds(5));

        assertThat(resp).containsExactly(fakeArrow);
    }

    private static Mono<byte[]> readAllBytesOrFail(WebTestClient.ResponseSpec spec, String debug) {
        return DataBufferUtils.join(spec.returnResult(DataBuffer.class).getResponseBody())
                .map(buf -> {
                    try {
                        byte[] out = new byte[buf.readableByteCount()];
                        buf.read(out);
                        return out;
                    } finally {
                        DataBufferUtils.release(buf);
                    }
                })
                .switchIfEmpty(Mono.error(new AssertionError("Response body was empty (no bytes emitted)\n" + debug)));
    }

    private static String dumpRecordedRequests(RecordedRequest... requests) {
        List<String> lines = new ArrayList<>();
        for (int i = 0; i < requests.length; i++) {
            RecordedRequest r = requests[i];
            if (r == null) {
                lines.add("skadiRequest[" + i + "]=null");
                continue;
            }
            lines.add("skadiRequest[" + i + "].method=" + r.getMethod());
            lines.add("skadiRequest[" + i + "].path=" + r.getPath());
            lines.add("skadiRequest[" + i + "].headers=" + r.getHeaders());
        }
        return String.join("\n", lines);
    }
}