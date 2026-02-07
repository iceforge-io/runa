package org.iceforge.runa.semantic.config;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "runa")
public class RunaProperties {

    /**
     * Base URL of the Skadi server, e.g. http://localhost:8080
     */
    @NotBlank
    private String skadiBaseUrl = "http://localhost:8080";

    /**
     * Path used to submit a query to Skadi. Default assumes Skadi v1 API.
     */
    @NotBlank
    private String skadiSubmitPath = "/api/v1/queries";

    /**
     * Path template used to fetch Arrow results. If empty, SCUDI will attempt to proxy Arrow directly from submit.
     *
     * Use {queryId} token, e.g. /api/v1/queries/{queryId}/results/arrow
     */
    private String skadiArrowResultPathTemplate = "/api/v1/queries/{queryId}/results/arrow";

    /**
     * Location of the semantic model YAML on the classpath.
     */
    @NotBlank
    private String modelResource = "semantic-model.yml";

    public String getSkadiBaseUrl() {
        return skadiBaseUrl;
    }

    public void setSkadiBaseUrl(String skadiBaseUrl) {
        this.skadiBaseUrl = skadiBaseUrl;
    }

    public String getSkadiSubmitPath() {
        return skadiSubmitPath;
    }

    public void setSkadiSubmitPath(String skadiSubmitPath) {
        this.skadiSubmitPath = skadiSubmitPath;
    }

    public String getSkadiArrowResultPathTemplate() {
        return skadiArrowResultPathTemplate;
    }

    public void setSkadiArrowResultPathTemplate(String skadiArrowResultPathTemplate) {
        this.skadiArrowResultPathTemplate = skadiArrowResultPathTemplate;
    }

    public String getModelResource() {
        return modelResource;
    }

    public void setModelResource(String modelResource) {
        this.modelResource = modelResource;
    }
}
