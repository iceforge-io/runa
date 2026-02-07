package org.iceforge.runa.semantic.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.iceforge.runa.semantic.config.RunaProperties;
import org.iceforge.runa.semantic.model.SemanticModel;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

@Component
public class SemanticModelLoader {

    private final ObjectMapper yamlMapper;
    private final RunaProperties props;

    private volatile SemanticModel cached;

    public SemanticModelLoader(ObjectMapper yamlObjectMapper, RunaProperties props) {
        this.yamlMapper = Objects.requireNonNull(yamlObjectMapper);
        this.props = Objects.requireNonNull(props);
    }

    public SemanticModel load() {
        SemanticModel local = cached;
        if (local != null) return local;

        synchronized (this) {
            if (cached != null) return cached;
            try (InputStream in = new ClassPathResource(props.getModelResource()).getInputStream()) {
                cached = yamlMapper.readValue(in, SemanticModel.class);
                return cached;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to load semantic model resource: " + props.getModelResource(), e);
            }
        }
    }

    public void reload() {
        cached = null;
    }
}
