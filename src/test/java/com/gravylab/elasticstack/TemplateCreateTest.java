package com.gravylab.elasticstack;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.PutComposableIndexTemplateRequest;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TemplateCreateTest extends CommonTestClass {

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
                .order(1)
                ;


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
                .mapping(builder)
                ;

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




}
