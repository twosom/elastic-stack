package com.gravylab.elasticstack;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.AnalyzeResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.IndexTemplatesExistRequest;
import org.elasticsearch.search.SearchHit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class CommonTestClass {

    RestHighLevelClient client;


    @BeforeEach
    void beforeEach() {
        client = getDockerRestClient();
    }

    @AfterEach
    void afterEach() throws IOException {
        client.close();
    }

    private RestHighLevelClient getDockerRestClient() {
        RestClientBuilder builder = RestClient.builder(
                HttpHost.create("localhost:9200")
        );
        return new RestHighLevelClient(
                builder
                        .setHttpClientConfigCallback(
                                httpClientBuilder ->
                                        httpClientBuilder
                                                .setConnectionReuseStrategy((response, context) -> true)
                                                .setKeepAliveStrategy((response, context) -> 9999999999L)
                        )
        );
    }


    protected void removeIfExistsTemplate(String templateName) throws IOException {
        IndexTemplatesExistRequest indexTemplatesExistRequest = new IndexTemplatesExistRequest(templateName);
        boolean existsTemplate = client.indices().existsTemplate(indexTemplatesExistRequest, RequestOptions.DEFAULT);
        if (existsTemplate) {
            DeleteIndexTemplateRequest deleteIndexTemplateRequest = new DeleteIndexTemplateRequest(templateName);
            AcknowledgedResponse acknowledgedResponse = client.indices().deleteTemplate(deleteIndexTemplateRequest, RequestOptions.DEFAULT);
            assertTrue(acknowledgedResponse.isAcknowledged());
        }
    }

    protected void removeIfExistsIndex(String indexName) throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        if (exists) {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
            AcknowledgedResponse acknowledgedResponse = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            assertTrue(acknowledgedResponse.isAcknowledged());
        }
    }

    protected void printAnalyzeResponse(AnalyzeResponse analyzeResponse) {
        analyzeResponse
                .getTokens()
                .stream()
                .map(AnalyzeResponse.AnalyzeToken::getTerm)
                .forEach(System.err::println);
    }

    protected void printSearchResponse(SearchResponse searchResponse) {
        Arrays.stream(searchResponse
                        .getInternalResponse()
                        .hits()
                        .getHits())
                .map(SearchHit::getSourceAsMap)
                .forEach(System.err::println);
    }
}
