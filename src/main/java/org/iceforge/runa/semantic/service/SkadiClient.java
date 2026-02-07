package org.iceforge.runa.semantic.service;

import org.iceforge.runa.semantic.config.RunaProperties;
import org.iceforge.runa.semantic.web.SkadiQueryRequest;
import org.iceforge.runa.semantic.web.SkadiSubmitResponse;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

@Component
public class SkadiClient {

    public static final MediaType ARROW_STREAM = MediaType.parseMediaType("application/vnd.apache.arrow.stream");

    private final WebClient webClient;
    private final RunaProperties props;

    public SkadiClient(WebClient skadiWebClient, RunaProperties props) {
        this.webClient = Objects.requireNonNull(skadiWebClient);
        this.props = Objects.requireNonNull(props);
    }

    /**
     * Submit query to Skadi.
     * If Skadi directly returns Arrow stream, this returns that stream.
     * Otherwise it parses JSON response, extracts queryId, and fetches Arrow from results endpoint.
     */
    public Mono<Flux<DataBuffer>> runArrow(SkadiQueryRequest submitBody) {
        return webClient.post()
                .uri(props.getSkadiSubmitPath())
                .contentType(MediaType.APPLICATION_JSON)
                .accept(ARROW_STREAM, MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue(submitBody))
                .exchangeToMono(resp -> handleSubmitResponse(resp, submitBody));
    }

    private Mono<Flux<DataBuffer>> handleSubmitResponse(ClientResponse resp, SkadiQueryRequest submitBody) {
        MediaType ct = resp.headers().contentType().orElse(MediaType.APPLICATION_OCTET_STREAM);
        if (ARROW_STREAM.isCompatibleWith(ct)) {
            // Direct Arrow stream from submit endpoint (ideal).
            return Mono.just(resp.bodyToFlux(DataBuffer.class));
        }

        // Otherwise assume JSON: {"queryId":"..."} and then fetch Arrow.
        return resp.bodyToMono(SkadiSubmitResponse.class)
                .flatMap(r -> {
                    if (r == null || !StringUtils.hasText(r.getQueryId())) {
                        return Mono.error(new IllegalStateException("Skadi submit did not return Arrow and queryId was missing."));
                    }
                    String path = props.getSkadiArrowResultPathTemplate().replace("{queryId}", r.getQueryId());
                    return Mono.just(fetchArrow(path));
                });
    }

    private Flux<DataBuffer> fetchArrow(String uriPath) {
        return webClient.get()
                .uri(uriPath)
                .accept(ARROW_STREAM)
                .retrieve()
                .bodyToFlux(DataBuffer.class);
    }
}
