package com.gravylab.elasticstack;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
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

    @DisplayName("검색을 위한 인덱싱")
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


    @DisplayName("하나의 용어를 검색하는 매칭쿼리")
    @Test
    void matching_for_one_term() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        //TODO 전문(full text) 쿼리의 경우 검색어도 토큰화되기 때문에 검색어 Mary 는 [mary] 로 토큰화된다.
                                        // 분석기 종류에 따라 다르지만 일반적인 분석기를 사용했다면 대문자를 소문자로 변경하기 때문이다.
                                        matchQuery(CUSTOMER_FULL_NAME, "Mary")
                                )
                                .fetchSource(new String[]{CUSTOMER_FULL_NAME}, null)
                );


        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("복수 개의 용어를 검색하는 매치 쿼리")
    @Test
    void matching_for_many_terms() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        //TODO 검색어인 mary bailey 는 분석기에 의해 [mary, bailey] 로 토큰화된다.
                                        // 그리고 매치 쿼리에서 용어들 간의 공백은 OR 로 인식한다.
                                        // 즉, customer_full_name 필드에 mary 나 bailey 가 하나라도 포함된 도큐먼트가 있다면 매칭되었다고 판단한다.
                                        matchQuery(CUSTOMER_FULL_NAME, "mary bailey")
                                )
                                .fetchSource(new String[]{CUSTOMER_FULL_NAME}, null)
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }


    @DisplayName("operator 를 and 로 설정한 매치 검색")
    @Test
    void matching_with_operator_parameter() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        //TODO operator 파라미터는 기본값이 or 이기 때문에 operator 를 명시적으로 지정하지 안흥면 or 로 인식된다.
                                        matchQuery(CUSTOMER_FULL_NAME, "mary bailey")
                                                .operator(Operator.AND)
                                )
                                .fetchSource(new String[]{CUSTOMER_FULL_NAME}, null)
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }


    @DisplayName("매치 프레이즈 쿼리")
    @Test
    void search_with_match_phrase() throws Exception {
        SearchRequest searchRequest = new SearchRequest(KIBANA_SAMPLE_DATA_ECOMMERCE)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        //TODO 검색어인 mary bailey 가 [mary, bailey] 로 토큰화되는 것까지는 매치 쿼리와 같지만,
                                        // 매치 프레이즈 쿼리는 검색어에 사용된 용어들이 모두 포함되면서 용어의 순서까지 맞아야 한다.
                                        matchPhraseQuery(CUSTOMER_FULL_NAME, "mary bailey")
                                )
                                .fetchSource(new String[]{CUSTOMER_FULL_NAME}, null)
                );

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

    @DisplayName("텍스트 타입 필드에 대한 용어 쿼리")
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

    @DisplayName("텍스트 타입 필드에 대한 용어 쿼리")
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

    @DisplayName("텍스트 타입 필드에 대한 용어 쿼리")
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


    @DisplayName("용어들 쿼리")
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

    @DisplayName("여러 필드에 쿼리 요청하기")
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

    @DisplayName("와일드카드를 이용한 멀티 필드에 쿼리 요청하기")
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

    @DisplayName("가중치를 이용한 검색")
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

    @DisplayName("날짜/시간 범위 쿼리")
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

    @DisplayName("날짜/시간 범위 쿼리")
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

    @DisplayName("날짜/시간 범위 타입을 갖는 인덱스 생성")
    @Test
    void create_index_with_date_range_field() throws Exception {

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

    @DisplayName("날짜/시간 범위를 갖는 도큐먼트 인덱싱")
    @Test
    void indexing_data_for_date_range_field() throws Exception {
        create_index_with_date_range_field();
        IndexRequest indexRequest = new IndexRequest(RANGE_TEST_INDEX)
                .id("1")
                .source(Map.of(TEST_DATE, Map.of("gte", "2021-01-21", "lt", "2021-01-25")));

        client.index(indexRequest, RequestOptions.DEFAULT);
    }

    @DisplayName("relation 파라미터를 이용한 범위 설정")
    @Test
    void search_with_range_query_with_relation_parameter() throws Exception {
        SearchRequest searchRequest = new SearchRequest(RANGE_TEST_INDEX)
                .source(
                        new SearchSourceBuilder()
                                .query(
                                        rangeQuery(TEST_DATE)
                                                .gte("2021-01-21")
                                                .lte("2021-01-28")
                                                .relation("within")
                                )
                );
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        printSearchResponse(searchResponse);
    }

}
