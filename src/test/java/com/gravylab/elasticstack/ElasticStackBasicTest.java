package com.gravylab.elasticstack;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ElasticStackBasicTest extends CommonTestClass {

    public static final String TEST_TEMPLATE = "test_template";
    public static final String INDEX_PATTERNS = "index_patterns";
    public static final String PRIORITY = "priority";
    public static final String TEMPLATE = "template";
    public static final String SETTINGS = "settings";
    public static final String NUMBER_OF_SHARDS = "number_of_shards";
    public static final String NUMBER_OF_REPLICAS = "number_of_replicas";
    public static final String MAPPINGS = "mappings";
    public static final String PROPERTIES = "properties";
    public static final String NAME = "name";
    public static final String TEXT = "text";
    public static final String TYPE = "type";
    public static final String AGE = "age";
    public static final String SHORT = "short";
    public static final String GENDER = "gender";
    public static final String KEYWORD = "keyword";
    public static final String TEST_INDEX_1 = "test_index1";
    public static final String MULTI_TEMPLATE_1 = "multi_template1";
    public static final String INTEGER = "integer";
    public static final String MULTI_TEMPLATE_2 = "multi_template2";
    public static final String MULTI_DATA_INDEX = "multi_data_index";
    public static final String DYNAMIC_INDEX_1 = "dynamic_index1";
    public static final String DYNAMIC_TEMPLATES = "dynamic_templates";
    public static final String MY_STRING_FIELDS = "my_string_fields";
    public static final String MATCH_MAPPING_TYPE = "match_mapping_type";
    public static final String STRING = "string";
    public static final String MAPPING = "mapping";
    public static final String DYNAMIC_INDEX_2 = "dynamic_index2";
    public static final String MY_LONG_FIELDS = "my_long_fields";
    public static final String MATCH = "match";
    public static final String UNMATCH = "unmatch";
    public static final String LONG = "long";
    public static final String STOP = "stop";
    public static final String STANDARD = "standard";
    public static final String UPPERCASE = "uppercase";
    public static final String CUSTOMER_ANALYZER = "customer_analyzer";
    public static final String ANALYSIS = "analysis";
    public static final String ANALYZER = "analyzer";
    public static final String FILTER = "filter";
    public static final String CUSTOM = "custom";
    public static final String CHAR_FILTER = "char_filter";
    public static final String LOWERCASE = "lowercase";
    public static final String TOKENIZER = "tokenizer";
    public static final String MY_STOPWORDS = "my_stopwords";
    public static final String MY_ANALYZER = "my_analyzer";
    public static final String STOPWORDS = "stopwords";

    @DisplayName("test_template 인덱스 템플릿 생성")
    @Test
    void create_test_template() throws Exception {
        removeIfExistsTemplate(TEST_TEMPLATE);
        Settings.Builder settings = Settings.builder()
                .put(NUMBER_OF_SHARDS, 3)
                .put(NUMBER_OF_REPLICAS, 1);


        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject();
        {
            mappingBuilder.startObject(PROPERTIES);
            {
                mappingBuilder.startObject(NAME);
                {
                    mappingBuilder.field(TYPE, TEXT);
                }
                mappingBuilder.endObject();

                mappingBuilder.startObject(AGE);
                {
                    mappingBuilder.field(TYPE, SHORT);
                }
                mappingBuilder.endObject();

                mappingBuilder.startObject(GENDER);
                {
                    mappingBuilder.field(TYPE, KEYWORD);
                }
                mappingBuilder.endObject();
            }
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject();
        PutIndexTemplateRequest putIndexTemplateRequest = new PutIndexTemplateRequest(TEST_TEMPLATE)
                .mapping(mappingBuilder)
                .settings(settings)
                .patterns(List.of("test_*"))
                .order(1);


        AcknowledgedResponse putTemplateResponse = client.indices().putTemplate(putIndexTemplateRequest, RequestOptions.DEFAULT);
        assertTrue(putTemplateResponse.isAcknowledged());
    }

    @DisplayName("생성한 템플릿 패턴에 맞는 인덱스 생성")
    @Test
    void create_index_for_template() throws Exception {
        removeIfExistsIndex(TEST_INDEX_1);


        IndexRequest indexRequest = new IndexRequest(TEST_INDEX_1)
                .id("1")
                .source(
                        Map.of(NAME, "kim",
                                AGE, 10,
                                GENDER, "male")
                );

        IndexResponse indexResponse
                = client.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println("indexResponse = " + indexResponse);
    }

    @DisplayName("템플릿 우선순위")
    @Test
    void template_priority() throws Exception {
        removeIfExistsTemplate(MULTI_TEMPLATE_1);
        removeIfExistsTemplate(MULTI_TEMPLATE_2);


        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject(PROPERTIES);
            {
                builder.startObject(AGE);
                {
                    builder.field(TYPE, INTEGER);
                }
                builder.endObject();
                builder.startObject(NAME);
                {
                    builder.field(TYPE, TEXT);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        PutIndexTemplateRequest putIndexTemplateRequest = new PutIndexTemplateRequest(MULTI_TEMPLATE_1)
                .patterns(List.of("multi_*"))
                .order(1)
                .mapping(builder);

        AcknowledgedResponse acknowledgedResponse = client.indices().putTemplate(putIndexTemplateRequest, RequestOptions.DEFAULT);
        assertTrue(acknowledgedResponse.isAcknowledged());

        builder = XContentFactory.jsonBuilder();

        builder.startObject();
        {
            builder.startObject(PROPERTIES);
            {
                builder.startObject(NAME);
                {
                    builder.field(TYPE, KEYWORD);
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        putIndexTemplateRequest = new PutIndexTemplateRequest(MULTI_TEMPLATE_2)
                .order(2)
                .patterns(List.of("multi_data_*"))
                .mapping(builder)
        ;

        acknowledgedResponse = client.indices().putTemplate(putIndexTemplateRequest, RequestOptions.DEFAULT);
        assertTrue(acknowledgedResponse.isAcknowledged());

        CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest(MULTI_DATA_INDEX), RequestOptions.DEFAULT);
        System.out.println("createIndexResponse = " + createIndexResponse);
    }

    @DisplayName("다이내믹 매핑을 적용한 인덱스 생성")
    @Test
    void create_index_using_dynamic_mapping() throws Exception {
        removeIfExistsIndex(DYNAMIC_INDEX_1);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startArray(DYNAMIC_TEMPLATES);
            {
                builder.startObject();
                {
                    builder.startObject(MY_STRING_FIELDS);
                    {
                        //TODO match_mapping_type 은 조건문 혹은 매핑 트리거다. 조건에 만족할 경우 트리거링이 된다.
                        // 여기서는 문자열 타입 데이터가 있으면 조건에 만족한다.
                        // if(타입이 스트링이면) 키워드로 매핑해라.
                        builder.field(MATCH_MAPPING_TYPE, STRING);
                        //TODO mapping 은 실제 매핑을 적용하는 부분이다. 문자열 타입의 데이터가 들어오면 키워드 타입으로 매핑한다.
                        builder.startObject(MAPPING);
                        {
                            builder.field(TYPE, KEYWORD);
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(DYNAMIC_INDEX_1)
                .mapping(builder);
        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertTrue(createIndexResponse.isAcknowledged());
    }

    @DisplayName("다이내믹 매핑이 적용된 인덱스에 도큐먼트 인덱싱")
    @Test
    void indexing_to_dynamic_mapping_index() throws Exception {
        create_index_using_dynamic_mapping();
        IndexRequest indexRequest = new IndexRequest(DYNAMIC_INDEX_1)
                .source(Map.of(NAME, "mr. kim", AGE, 40));
        client.index(indexRequest, RequestOptions.DEFAULT);

        GetMappingsRequest getMappingRequest = new GetMappingsRequest()
                .indices(DYNAMIC_INDEX_1);

        GetMappingsResponse getMappingResponse = client.indices().getMapping(getMappingRequest, RequestOptions.DEFAULT);
        getMappingResponse
                .mappings()
                .values()
                .stream()
                .map(MappingMetadata::getSourceAsMap)
                .forEach(System.err::println);
    }

    @DisplayName("다이내믹 템플릿 match, unmatch 조건문 이용")
    @Test
    void create_index_using_dynamic_template_with_match_unmatch() throws Exception {
        removeIfExistsIndex(DYNAMIC_INDEX_2);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startArray(DYNAMIC_TEMPLATES);
            {
                builder.startObject();
                {
                    //TODO my_long_fields 라는 이름의 다이내믹 템플릿을 적용했다.
                    builder.startObject(MY_LONG_FIELDS);
                    {
                        //TODO match 는 정규표현식을 이용해 필드명을 검사할 수 있다.
                        // match 는 조건에 맞는 경우 mapping 에 의해 필드들은 모두 숫자(long) 타입을 갖는다.
                        builder.field(MATCH, "long_*");
                        //TODO unmatch 는 조건에 맞는 경우 mapping 에서 제외한다.
                        builder.field(UNMATCH, "*_text");
                        builder.startObject(MAPPING);
                        {
                            builder.field(TYPE, LONG);
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(DYNAMIC_INDEX_2)
                .mapping(builder);
        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertTrue(createIndexResponse.isAcknowledged());
    }

    @DisplayName("다이내믹 매핑이 적용도니 dyanmic_index2 인덱스에 도큐먼트 인덱싱")
    @Test
    void indexing_to_dynamic_mapping_index2() throws Exception {
        create_index_using_dynamic_template_with_match_unmatch();
        IndexRequest indexRequest = new IndexRequest(DYNAMIC_INDEX_2)
                .source(Map.of("long_num", 5,
                        "long_text", 170));

        client.index(indexRequest, RequestOptions.DEFAULT);

    }

    @DisplayName("스톱 분석기 테스트")
    @Test
    void test_with_stop_analyzer() throws Exception {
        //TODO stop 분석기는 소문자 변경 (lowercase) 토크나이저와 스톱 (stop) 토큰 필터로 구성되어 있다.
        AnalyzeRequest analyzeRequest = AnalyzeRequest.withGlobalAnalyzer(STOP, "The 10 most loving dog breeds");
        AnalyzeResponse analyzeResponse = client.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        printAnalyzeResponse(analyzeResponse);

        //TODO 엘라스틱서치에서 자주 사용되는 분석기
        // standard : 특별한 설정이 없으면 엘라스틱서치가 기본적으로 사용하는 분석기다. 영문법을 기준으로 한 스탠다드 토크나이저와 소문자 변경 필터, 스톰 필터가 포함되어있다.
        // simple : 문자만 토큰화한다. 공백, 숫자, 하이픈(-) 이나 작은따옴표(') 같은 문자는 토큰화하지 않는다.
        // whitespace : 공백을 기준으로 구분하여 토큰화한다.
        // stop : simple 분석기와 비슷하지만 스톱 필터가 포함되었다.


        //TODO 엘라스틱서치에서 자주 사용되는 토크나이저
        // standard : 스탠다드 분석기가 사용하는 토크나이저로, 특별한 설정이 없으면 기본 토크나이저로 사용된다. 쉼표(,) 나 점(.) 같은 기호를 제거하며 텍스트 기반으로 토큰화한다.
        // lowercase: 텍스트 기반으로 토큰화하며 모든 문자를 소문자로 변경해 토큰화한다.
        // ngram : 원문으로부터 N개의 연속된 글자 단위를 모두 토큰화한다. 예를 들어, '엘라스틱서치' 라는 원문을 2gram 으로 토큰화한다면 [엘라, 라스, 스틱, 틱서, 서치] 와 같이 연속된 두 글자를 모두 추출한다.
        // 사실상 원문으로부터 검색할 수 있는 거의 모든 조합을 얻어낼 수 있기 때문에 정밀한 부분 검색에 강점이 있지만, 토크나이징을 수행한 N개 이하의 글자 수로는 검색이 불가능하며 모든 조합을 추출하기 때문에 저장공간을 많이 차지한다는 단점이 있다.
        // uax_url_email : 스탠다드 분석기와 비슷하지만 url 이나 이메일을 토큰화하는 데 강점이 있다.
    }


    @DisplayName("uax_url_email 토크나이저 테스트")
    @Test
    void uax_url_email_tokenizer_test() throws Exception {
        AnalyzeRequest analyzeRequest = AnalyzeRequest.buildCustomAnalyzer("uax_url_email")
                .build("email : elastic@elk-company.com");

        AnalyzeResponse analyzeResponse = client.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        printAnalyzeResponse(analyzeResponse);
    }

    @DisplayName("uppercase 필터 테스트")
    @Test
    void uppercase_filter_test() throws Exception {
        AnalyzeRequest analyzeRequest = AnalyzeRequest.buildCustomAnalyzer(STANDARD)
                .addTokenFilter(UPPERCASE)
                .build("The 10 most loving dog breeds");

        AnalyzeResponse analyzeResponse = client.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
        printAnalyzeResponse(analyzeResponse);
    }

    @DisplayName("커스텀 분석기를 적용한 customer_analyzer 인덱스 생성")
    @Test
    void create_index_customer_analyzer() throws Exception {
        removeIfExistsIndex(CUSTOMER_ANALYZER);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {


            builder.startObject(ANALYSIS);
            {
                builder.startObject(FILTER);
                {
                    builder.startObject(MY_STOPWORDS);
                    {
                        builder.field(TYPE, STOP);
                        builder.startArray(STOPWORDS);
                        {
                            builder.value("lions");
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();


                builder.startObject(ANALYZER);
                {
                    builder.startObject(MY_ANALYZER);
                    {
                        builder.field(TYPE, CUSTOM);
                        builder.field(CHAR_FILTER, new ArrayList<String>());
                        builder.field(TOKENIZER, STANDARD);
                        builder.startArray(FILTER);
                        {
                            //TODO 커스텀 분석기에 필터를 적용할 때는 필터들의 순서에 유의해야 하고,
                            // 가능하면 모든 문자를 소문자로 변환한 후에 필터를 적용하는 것이 실수를 줄인다.
                            builder.value(LOWERCASE)
                                    .value(MY_STOPWORDS);
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();


        CreateIndexRequest createIndexRequest = new CreateIndexRequest(CUSTOMER_ANALYZER)
                .settings(builder);

        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertTrue(createIndexResponse.isAcknowledged());
    }

    @DisplayName("my_analyzer 커스텀 분석기 테스트")
    @Test
    void analyze_with_my_analyzer() throws Exception {
        create_index_customer_analyzer();
        AnalyzeRequest analyzeRequest = AnalyzeRequest.withIndexAnalyzer(CUSTOMER_ANALYZER, MY_ANALYZER, "Cats Lions Dogs");
        AnalyzeResponse analyzeResponse = client.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);

        printAnalyzeResponse(analyzeResponse);
    }


}
