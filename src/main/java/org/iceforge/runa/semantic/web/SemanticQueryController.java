package org.iceforge.runa.semantic.web;

import jakarta.validation.Valid;
import org.iceforge.runa.semantic.service.SemanticSqlCompiler;
import org.iceforge.runa.semantic.service.SemanticQueryService;
import org.iceforge.runa.semantic.service.SkadiClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

@RestController
@RequestMapping("/api/semantic")
public class SemanticQueryController {

    private final SemanticQueryService service;

    /**
     * For POC we allow JDBC to be configured in application.yml and/or overridden per request later.
     */
    private final SkadiQueryRequest.JdbcConfig jdbcCfg;

    public SemanticQueryController(SemanticQueryService service,
                                  @Value("${warehouse.jdbcUrl}") String jdbcUrl,
                                  @Value("${warehouse.username:}") String username,
                                  @Value("${warehouse.password:}") String password) {
        this.service = Objects.requireNonNull(service);
        this.jdbcCfg = new SkadiQueryRequest.JdbcConfig();
        this.jdbcCfg.setJdbcUrl(jdbcUrl);
        this.jdbcCfg.setUsername(username);
        this.jdbcCfg.setPassword(password);
    }

    /**
     * Compiles a semantic request to SQL, executes via Skadi, and streams Arrow IPC back.
     */
    @PostMapping(value = "/query", produces = "application/vnd.apache.arrow.stream")
    public Mono<ResponseEntity<Flux<DataBuffer>>> query(@Valid @RequestBody SemanticQueryRequest req) {
        return service.runArrow(req, jdbcCfg)
                .map(body -> ResponseEntity.ok()
                        .header(HttpHeaders.CONTENT_TYPE, SkadiClient.ARROW_STREAM.toString())
                        .body(body));
    }

    @ExceptionHandler(SemanticSqlCompiler.SemanticValidationException.class)
    public ResponseEntity<ErrorResponse> badRequest(RuntimeException e) {
        return ResponseEntity.badRequest().contentType(MediaType.APPLICATION_JSON)
                .body(new ErrorResponse("VALIDATION_ERROR", e.getMessage()));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<ErrorResponse> serverError(RuntimeException e) {
        return ResponseEntity.internalServerError().contentType(MediaType.APPLICATION_JSON)
                .body(new ErrorResponse("SERVER_ERROR", e.getMessage()));
    }
}
