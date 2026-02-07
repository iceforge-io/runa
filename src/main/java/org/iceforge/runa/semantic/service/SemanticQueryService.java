package org.iceforge.runa.semantic.service;

import org.iceforge.runa.semantic.model.SemanticModel;
import org.iceforge.runa.semantic.web.SemanticQueryRequest;
import org.iceforge.runa.semantic.web.SkadiQueryRequest;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

@Service
public class SemanticQueryService {

    private final SemanticModelLoader loader;
    private final SemanticSqlCompiler compiler;
    private final SkadiClient skadiClient;

    public SemanticQueryService(SemanticModelLoader loader,
                               SemanticSqlCompiler compiler,
                               SkadiClient skadiClient) {
        this.loader = Objects.requireNonNull(loader);
        this.compiler = Objects.requireNonNull(compiler);
        this.skadiClient = Objects.requireNonNull(skadiClient);
    }

    public Mono<Flux<DataBuffer>> runArrow(SemanticQueryRequest req, SkadiQueryRequest.JdbcConfig jdbcCfg) {
        SemanticModel model = loader.load();
        SemanticSqlCompiler.CompiledQuery cq = compiler.compile(model, req);

        SkadiQueryRequest skadiReq = new SkadiQueryRequest();
        skadiReq.setJdbc(jdbcCfg);
        skadiReq.setSql(cq.sql());

        return skadiClient.runArrow(skadiReq);
    }
}
