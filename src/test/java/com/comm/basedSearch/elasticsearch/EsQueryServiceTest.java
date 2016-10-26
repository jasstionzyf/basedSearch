/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.comm.basedSearch.elasticsearch;

import com.comm.basedSearch.entity.EsCommonQuery;
import com.comm.basedSearch.entity.QueryItem;
import com.comm.basedSearch.entity.SortItem;
import com.comm.basedSearch.entity.SubQuery;
import com.comm.basedSearch.utils.Instances;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.junit.*;

import java.io.StringReader;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 *
 * @author jasstion
 */
public class EsQueryServiceTest {
    public static final NodeTestUtils   nodeTestUtils=new NodeTestUtils();



    public EsQueryServiceTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {


    }

    @After
    public void tearDown() {

    }

    /**
     * Test of processQuery method, of class EsQueryService.
     */
    @Test
    public void testProcessQuery() throws Exception {
    }

    /**
     * Test of query method, of class EsQueryService.
     */
    @Test
    public void testQuery() throws Exception {
      Node node=  nodeTestUtils.createNode();
        nodeTestUtils.releaseNode(node);
        node=  nodeTestUtils.createNode();

        Client client=node.client();
        String indexName="com";
        String typeName="user";

        Settings indexSettings = Settings.settingsBuilder()
                .put("index.similarity.payload.type", "payload_similarity")
                .put("analysis.analyzer.payloads.type", "custom")
                .put("analysis.analyzer.payloads.tokenizer", "whitespace")
                .put("analysis.analyzer.payloads.filter.0", "lowercase")
                .put("analysis.analyzer.payloads.filter.1", "delimited_payload_filter")
                .build();
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject(typeName)
                .startObject("properties")
                .startObject("des")
                .field("type", "string")
                .field("analyzer", "payloads")
                .field("term_vector", "with_positions_offsets_payloads")
                .field("similarity", "payload") // resolves to org.elasticsearch.index.similarity.PayloadSimilarity
                .endObject()
                .startObject("name")
                .field("type", "string")
                .endObject()
                .startObject("userId")
                .field("type", "integer")
                .endObject()
                .startObject("age")
                .field("type", "integer")
                .endObject()

                .endObject()
                .endObject()
                .endObject().string();

        client.admin().indices()
                .prepareCreate(indexName)
                .setSettings(indexSettings)
                .addMapping(typeName, mapping)
                .execute().actionGet();

        client.prepareIndex(indexName, typeName, "1")
                .setSource(XContentFactory.jsonBuilder().startObject()
                                .field("des", "box|5.0 swimming")
                                .field("name","jack award")
                                .field("age",123)
                                .field("userId",1)
                                .endObject()
                )
                .setRefresh(true)
                .execute().actionGet();
        client.prepareIndex(indexName, typeName, "2")
                .setSource(XContentFactory.jsonBuilder().startObject()
                                .field("des", "box|10.0 swimming")
                                .field("name","jack award")
                                .field("age",123)
                                .field("userId",2)
                                .endObject()
                )
                .setRefresh(true)
                .execute().actionGet();
        client.prepareIndex(indexName, typeName, "3")
                .setSource(XContentFactory.jsonBuilder().startObject()
                                .field("des", " box boss|10.0 basket")
                                .field("name","jack award")
                                .field("age",1230)
                                .field("userId",3)
                                .endObject()
                )
                .setRefresh(true)
                .execute().actionGet();
        client.prepareIndex(indexName, typeName, "5")
                .setSource(XContentFactory.jsonBuilder().startObject()
                                .field("des", "boss|100 box |10.0 basket|100")
                                .field("name","jack award")
                                .field("age",123000)
                                .field("userId",5)
                                .endObject()
                )
                .setRefresh(true)
                .execute().actionGet();

        client.prepareIndex(indexName, typeName, "4")
                .setSource(XContentFactory.jsonBuilder().startObject()
                                .field("des", "boss|100 box|10.0 football")
                                .field("name","jack award")
                                .field("age",12300)
                                .field("userId",4)
                                .endObject()
                )
                .setRefresh(true)
                .execute().actionGet();



        List<QueryItem> items = Lists.newArrayList();
        QueryItem queryItem=new QueryItem("des",Lists.newArrayList("box"));
        queryItem.setIsPayload(true);
       // items.add(queryItem);
        SubQuery subQuery=new SubQuery();
        subQuery.setLogic("AND");
        List<SubQuery> subQueries=Lists.newArrayList(new SubQuery("AND", new QueryItem("des", Lists.newArrayList("basket", "football"))));
        QueryItem queryItem1=new QueryItem("des", Lists.newArrayList("boss"));
        queryItem1.setIsPayload(true);
        SubQuery subQuery1=new SubQuery("NOT",queryItem1 );
        subQuery1.setSubQuerys(Lists.newArrayList(new SubQuery("AND", new QueryItem("age", Lists.newArrayList("1220TO1230")))));
        subQueries.add(subQuery1);
        subQuery.setSubQuerys(subQueries);


        final List<String> fls = Lists.newArrayList("userId","des","name","age");

        List<SortItem> sortItems = Lists.newArrayList();
        //logstash-2015.12.10 log4j
        //EsCommonQuery baiheQuery = new EsCommonQuery(items, 1, 18, sortItems, fls, "baihe_user", "user");
        EsCommonQuery baiheQuery = new EsCommonQuery(items, 1, 5, sortItems, fls, indexName, typeName);
        baiheQuery.setSubQuery(subQuery);
        baiheQuery.setScoreScript("100*_score");




        EsQueryGenerator.EsQueryWrapper esQueryWrapper= new EsQueryGenerator().generateFinalQuery(baiheQuery);

        System.out.print(esQueryWrapper.getSearchSourceBuilder().toString());
        EsQueryService esQueryService = new EsQueryService(client);
        List<Map<String, Object>> results = esQueryService.query(baiheQuery);
        System.out.print(results.size()+"\n");
        for (Map<String,Object> user:results) {
            Object content =  user.get("userId");
            //String c1=new String(content.getBytes(),"utf8");
            System.out.print(content+"\n");
            System.out.print(user.get("score")+"\n");
        }




        nodeTestUtils.releaseNode(node);


    }

}
