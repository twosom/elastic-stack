package com.gravylab.elasticstack;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.elasticsearch.index.query.QueryBuilders.*;

public class ElasticSearchTest extends CommonTestClass {

    public static final String KIBANA_SAMPLE_DATA_ECOMMERCE = "kibana_sample_data_ecommerce";
    public static final String CATEGORY = "category";
    public static final String DAY_OF_WEEK = "day_of_week";
    public static final String FRIDAY = "Friday";
    public static final String CUSTOMER_FULL_NAME = "customer_full_name";

    @DisplayName("쿼리 컨텍스트 실행")
    @Test
    void run_query_context() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        matchQuery(CATEGORY, "clothing")
                                )
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("필터 컨텍스트 실행")
    @Test
    void run_filter_context() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        boolQuery()
                                                .filter(
                                                        termQuery(DAY_OF_WEEK, FRIDAY)
                                                )
                                )
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }


    @DisplayName("쿼리 DSL 예시")
    @Test
    void run_query_dsl() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        matchQuery(CUSTOMER_FULL_NAME, "Mary")
                                )
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("설명이 포함된 쿼리 컨텍스트 실행")
    @Test
    void run_query_context_with_explain() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        matchQuery("products.product_name", "Pants")
                                )
                                .explain(true)
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Arrays.stream(searchResponse
                        .getInternalResponse()
                        .hits()
                        .getHits())
                .map(SearchHit::getExplanation)
                .forEach(System.err::println);
    }
}
