package com.gravylab.elasticstack;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ElasticSearchTest extends CommonTestClass {

    public static final String KIBANA_SAMPLE_DATA_ECOMMERCE = "kibana_sample_data_ecommerce";
    public static final String CATEGORY = "category";
    public static final String DAY_OF_WEEK = "day_of_week";
    public static final String FRIDAY = "Friday";
    public static final String CUSTOMER_FULL_NAME = "customer_full_name";
    public static final String CUSTOMER_FULL_NAME_KEYWORD = CUSTOMER_FULL_NAME + ".keyword";
    public static final String QINDEX = "qindex";
    public static final String CONTENTS = "contents";
    public static final String PROPERTIES = "properties";
    public static final String TEXT = "text";
    public static final String TYPE = "type";
    public static final String KEYWORD = "keyword";
    public static final String CUSTOMER_FIRST_NAME = "customer_first_name";
    public static final String CUSTOMER_LAST_NAME = "customer_last_name";
    public static final String KIBANA_SAMPLE_DATA_FLIGHTS = "kibana_sample_data_flights";
    public static final String TIMESTAMP = "timestamp";
    public static final String TEST_DATE = "test_date";
    public static final String DATE_RANGE = "date_range";
    public static final String RANGE_TEST_INDEX = "range_test_index";

    @DisplayName("?????? ???????????? ??????")
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

    @DisplayName("?????? ???????????? ??????")
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


    @DisplayName("?????? DSL ??????")
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

    @DisplayName("????????? ????????? ?????? ???????????? ??????")
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

    @DisplayName("????????? ?????? ?????????")
    @Test
    void index_data_for_search() throws Exception {
        removeIfExistsIndex(QINDEX);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject(PROPERTIES);
            {
                builder.startObject(CONTENTS);
                {
                    builder.field(TYPE, TEXT);
                }
                builder.endObject();

                builder.startObject(CATEGORY);
                {
                    builder.field(TYPE, KEYWORD);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(QINDEX)
                .mapping(builder);

        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertTrue(createIndexResponse.isAcknowledged());

        BulkRequest bulkRequest = new BulkRequest()
                .add(
                        new IndexRequest(QINDEX)
                                .id("1")
                                .source(Map.of(CONTENTS, "I Love Elastic Stack"))
                )
                .add(
                        new IndexRequest(QINDEX)
                                .id("2")
                                .source(Map.of(CATEGORY, "Tech"))
                );

        client.bulk(bulkRequest, RequestOptions.DEFAULT);

        Thread.sleep(1000);
        SearchRequest searchRequest = new SearchRequest(QINDEX)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        matchQuery(CONTENTS, "elastic world")
                                )
                );


        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);


        searchRequest = new SearchRequest(QINDEX)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        termQuery(CATEGORY, "tech")
                                )
                );

        searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        long totalHits = searchResponse
                .getHits()
                .getTotalHits()
                .value;
        assertEquals(0, totalHits);
    }


    @DisplayName("????????? ????????? ???????????? ????????????")
    @Test
    void matching_for_one_term() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        //TODO ??????(full text) ????????? ?????? ???????????? ??????????????? ????????? ????????? Mary ??? [mary] ??? ???????????????.
                                        // ????????? ????????? ?????? ???????????? ???????????? ???????????? ??????????????? ???????????? ???????????? ???????????? ????????????.
                                        matchQuery(CUSTOMER_FULL_NAME, "Mary")
                                )
                                .fetchSource(new String[]{CUSTOMER_FULL_NAME}, null)
                );


        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("?????? ?????? ????????? ???????????? ?????? ??????")
    @Test
    void matching_for_many_terms() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        //TODO ???????????? mary bailey ??? ???????????? ?????? [mary, bailey] ??? ???????????????.
                                        // ????????? ?????? ???????????? ????????? ?????? ????????? OR ??? ????????????.
                                        // ???, customer_full_name ????????? mary ??? bailey ??? ???????????? ????????? ??????????????? ????????? ?????????????????? ????????????.
                                        matchQuery(CUSTOMER_FULL_NAME, "mary bailey")
                                )
                                .fetchSource(new String[]{CUSTOMER_FULL_NAME}, null)
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }


    @DisplayName("operator ??? and ??? ????????? ?????? ??????")
    @Test
    void matching_with_operator_parameter() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        //TODO operator ??????????????? ???????????? or ?????? ????????? operator ??? ??????????????? ???????????? ????????? or ??? ????????????.
                                        matchQuery(CUSTOMER_FULL_NAME, "mary bailey")
                                                .operator(Operator.AND)
                                )
                                .fetchSource(new String[]{CUSTOMER_FULL_NAME}, null)
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }


    @DisplayName("?????? ???????????? ??????")
    @Test
    void search_with_match_phrase() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        //TODO ???????????? mary bailey ??? [mary, bailey] ??? ??????????????? ???????????? ?????? ????????? ?????????,
                                        // ?????? ???????????? ????????? ???????????? ????????? ???????????? ?????? ??????????????? ????????? ???????????? ????????? ??????.
                                        matchPhraseQuery(CUSTOMER_FULL_NAME, "mary bailey")
                                )
                                .fetchSource(new String[]{CUSTOMER_FULL_NAME}, null)
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("????????? ?????? ????????? ?????? ?????? ??????")
    @Test
    void search_with_term_query_for_text_1() throws Exception {
        SearchRequest seasrchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        termQuery(CUSTOMER_FULL_NAME, "Mary Bailey")
                                )
                                .fetchSource(new String[]{CUSTOMER_FULL_NAME}, null)
                );

        SearchResponse searchResponse = client.search(seasrchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("????????? ?????? ????????? ?????? ?????? ??????")
    @Test
    void search_with_query_for_text_2() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        termQuery(CUSTOMER_FULL_NAME, "Mary")
                                )
                                .fetchSource(new String[]{CUSTOMER_FULL_NAME}, null)
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("????????? ?????? ????????? ?????? ?????? ??????")
    @Test
    void search_with_query_for_test3() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        termQuery(CUSTOMER_FULL_NAME_KEYWORD, "Mary Bailey")
                                )
                                .fetchSource(new String[]{CUSTOMER_FULL_NAME}, null)
                );
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }


    @DisplayName("????????? ??????")
    @Test
    void search_with_terms_query() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        termsQuery(DAY_OF_WEEK, "Monday", "Sunday")
                                )
                                .fetchSource(new String[]{DAY_OF_WEEK}, null)
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("?????? ????????? ?????? ????????????")
    @Test
    void search_with_multi_match_query() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        multiMatchQuery("mary", CUSTOMER_FULL_NAME, CUSTOMER_FIRST_NAME, CUSTOMER_LAST_NAME)
                                )
                                .fetchSource(new String[]{CUSTOMER_FIRST_NAME, CUSTOMER_LAST_NAME, CUSTOMER_FULL_NAME}, null)
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("?????????????????? ????????? ?????? ????????? ?????? ????????????")
    @Test
    void search_with_multi_match_query_using_wild_card() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        multiMatchQuery("mary", "customer_*_name")
                                )
                                .fetchSource(new String[]{CUSTOMER_FIRST_NAME, CUSTOMER_LAST_NAME, CUSTOMER_FIRST_NAME}, null)
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("???????????? ????????? ??????")
    @Test
    void search_with_boost_option() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        multiMatchQuery("mary", CUSTOMER_FULL_NAME, CUSTOMER_FIRST_NAME, CUSTOMER_LAST_NAME)
                                                .field(CUSTOMER_FULL_NAME, 2)
                                )
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("??????/?????? ?????? ??????")
    @Test
    void search_with_range_query_using_date1() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_FLIGHTS)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        rangeQuery(TIMESTAMP)
                                                .gte("2021-08-27")
                                                .lt("2020-08-28")
                                )
                );
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("??????/?????? ?????? ??????")
    @Test
    void search_with_range_query_using_date2() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_FLIGHTS)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        rangeQuery(TIMESTAMP)
                                                .gte("now-1M")
                                )
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("??????/?????? ?????? ????????? ?????? ????????? ??????")
    @Test
    void create_index_have_date_range_type() throws Exception {
        removeIfExistsIndex(RANGE_TEST_INDEX);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject(PROPERTIES);
            {
                builder.startObject(TEST_DATE);
                {
                    builder.field(TYPE, DATE_RANGE);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(RANGE_TEST_INDEX)
                .mapping(builder);

        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertTrue(createIndexResponse.isAcknowledged());
    }

    @DisplayName("??????/?????? ????????? ?????? ???????????? ?????????")
    @Test
    void index_data_have_date_range_type() throws Exception {
        create_index_have_date_range_type();
        IndexRequest indexRequest = new IndexRequest(RANGE_TEST_INDEX)
                .source(
                        Map.of(
                                TEST_DATE, Map.of(
                                        "gte", "2021-01-21",
                                        "lt", "2021-01-25"
                                ))
                );

        client.index(indexRequest, RequestOptions.DEFAULT);

        Thread.sleep(900);
    }

    @DisplayName("relation ??????????????? ????????? ?????? ??????")
    @Test
    void search_request_with_date_range() throws Exception {
        index_data_have_date_range_type();
        SearchRequest searchRequest = new SearchRequest(RANGE_TEST_INDEX)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        rangeQuery(TEST_DATE)
                                                .gte("2021-01-21")
                                                .lte("2021-01-28")
                                                //TODO relation ??? ????????? ??? ?????? ??????
                                                // intersects(?????????) : ?????? ?????? ?????? ??????????????? ?????? ???????????? ???????????? ??????????????? ?????? ??????.
                                                // contains : ??????????????? ?????? ???????????? ?????? ?????? ?????? ?????? ???????????? ??????.
                                                // within : ??????????????? ?????? ???????????? ?????? ?????? ??? ?????? ?????? ????????? ??????.
                                                .relation("within")
                                )
                );
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }


    @DisplayName("????????? ????????? ???????????? must ??????")
    @Test
    void search_with_bool_query_must_type() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        boolQuery()
                                                .must(
                                                        matchQuery(CUSTOMER_FIRST_NAME, "mary")
                                                )
                                )
                );
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("?????? ?????? ????????? ???????????? must ??????")
    @Test
    void search_with_multi_bool_query() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        boolQuery()
                                                .must(
                                                        termQuery(DAY_OF_WEEK, "Sunday")
                                                )
                                                .must(
                                                        matchQuery(CUSTOMER_FULL_NAME, "mary")
                                                )
                                )
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("????????? ????????? ???????????? must_not ??????")
    @Test
    void search_with_must_not_bool_query() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        boolQuery()
                                                .mustNot(
                                                        matchQuery(CUSTOMER_FULL_NAME, "mary")
                                                )
                                )
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("?????? ????????? must_not ????????? ?????? ???????????? ??????")
    @Test
    void search_with_complex_bool_query() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        boolQuery()
                                                .must(
                                                        matchQuery(CUSTOMER_FIRST_NAME, "mary")
                                                )
                                                .mustNot(
                                                        termQuery(CUSTOMER_LAST_NAME, "bailey")
                                                )
                                )
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("????????? ????????? ???????????? should ??????")
    @Test
    void search_with_single_should_type() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        boolQuery()
                                                .should(
                                                        matchQuery(CUSTOMER_FIRST_NAME, "marh")
                                                )
                                )
                );
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("?????? ?????? ????????? ???????????? should ??????")
    @Test
    void search_with_multiple_should_type() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        boolQuery()
                                                .should(
                                                        termQuery(DAY_OF_WEEK, "Sunday")
                                                )
                                                .should(
                                                        matchQuery(CUSTOMER_FULL_NAME, "mary")
                                                )
                                )
                );
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }


    @DisplayName("must ????????? ???????????? ???????????? ??????")
    @Test
    void search_with_only_must() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        boolQuery()
                                                .must(
                                                        matchQuery(CUSTOMER_FULL_NAME, "mary")
                                                )
                                )
                );
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("must ??? should ????????? ?????? ???????????? ??????")
    @Test
    void search_with_must_and_should() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        boolQuery()
                                                .must(
                                                        matchQuery(CUSTOMER_FULL_NAME, "mary")
                                                )
                                                //TODO should ??? ????????? ??????????????? ?????? ????????? ???????????? ??? ??????.
                                                .should(
                                                        termQuery(DAY_OF_WEEK, "Monday")
                                                )
                                )
                );
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("????????? ????????? ???????????? filter ??????")
    @Test
    void search_with_filter_type() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        boolQuery()
                                                .filter(
                                                        rangeQuery("products.base_price")
                                                                .gte(30)
                                                                .lte(60)
                                                )
                                )
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("filter ??? must ????????? ?????? ???????????? ??????")
    @Test
    void search_with_filter_and_must() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        boolQuery()
                                                .filter(
                                                        termQuery(DAY_OF_WEEK, "Sunday")
                                                )
                                                .must(
                                                        matchQuery(CUSTOMER_FULL_NAME, "mary")
                                                )
                                )
                );
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("??????????????? ?????? ??????")
    @Test
    void search_with_wildcard() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        wildcardQuery("customer_full_name.keyword", "M?r*")
                                )
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }
}
