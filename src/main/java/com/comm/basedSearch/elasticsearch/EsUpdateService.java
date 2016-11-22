/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.comm.basedSearch.elasticsearch;

import com.comm.basedSearch.service.UpdateService;
import com.google.common.collect.Maps;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

/**
 *
 * @author jasstion
 */
public class EsUpdateService extends EsService implements UpdateService {







    /**
     *
     */
    public EsUpdateService() {
        super();

    }

    public EsUpdateService(String elasticSearchUrl){
           super(elasticSearchUrl);

    }
    public EsUpdateService(String elasticHosts,String userName,String passwd,String clusterName){
        super(elasticHosts,userName,passwd,clusterName);

    }

    private void checkParameters(Map<String, String> updateMap) {
        String id = (String) updateMap.get("id");
        String type = updateMap.get("type");
        String index = updateMap.get("index");
        //id == null ||
        if ( type == null || index == null) {
            throw new IllegalArgumentException("updated Info must contains id, type, index field infomation!");
        }

    }

    /**
     * used to batch update or add documents if document not existed.
     * @param updatedMaps
     * once failure throw exception must to be processed by caller
     */
    public void bulkUpdate(List<Map<String, String>> updatedMaps)throws Exception{

        BulkRequestBuilder bulkRequest = client.prepareBulk();

        for(Map<String, String> updateMap:updatedMaps){
            String id = (String) updateMap.get("id");
            String type = updateMap.get("type");
            String index = updateMap.get("index");
           if(id==null || type==null || index==null){
               continue;

           }


            Map<String,Object> finalUpdatedMap=Maps.newHashMap();
            finalUpdatedMap.put("doc",updateMap);
            finalUpdatedMap.put("doc_as_upsert","true");
            IndexRequestBuilder requestBuilder=client.prepareIndex().setId(id).setSource(finalUpdatedMap).setIndex(index).setType(type);
            bulkRequest.add(requestBuilder);




        }
        BulkResponse bulkResponse = bulkRequest.get();
        processBulkRequest(bulkResponse);









    }


    /**
     * used to batch add documents,do not care whether the input document has id field
     * @param updatedMaps
     */
    public void bulkAdd(List<Map<String, String>> updatedMaps)throws Exception{


        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for(Map<String, String> updateMap:updatedMaps){
            String id = (String) updateMap.get("id");
            String type = updateMap.get("type");
            String index = updateMap.get("index");
           IndexRequestBuilder requestBuilder= client.prepareIndex().setId(id).setSource(updateMap).setIndex(index).setType(type);
            bulkRequest.add(requestBuilder);




        }

        BulkResponse bulkResponse = bulkRequest.get();
        processBulkRequest(bulkResponse);










    }

    private void processBulkRequest(BulkResponse bulkResponse) throws Exception{
        if (bulkResponse.hasFailures()) {
            throw  new Exception(bulkResponse.toString());
        }


    }
    /**
     * used to update or save a document,  the input document must have id field, means the document's id
     * in elasticserch must be  manually specify.
     * @param updateMap
     */
    @Override
    public void update(Map<String, String> updateMap) {
        checkParameters(updateMap);
        String id = (String) updateMap.get("id");
        String type = updateMap.get("type");
        String index = updateMap.get("index");
       // String updateScript = generateEsUpdateScriptFromMap(updateMap);
       // System.out.print(updateScript+"\n");
       // Update update = new Update.Builder(updateScript).index(index).type(type).id(id).build();
        Map<String,Object> finalUpdatedMap=Maps.newHashMap();
        finalUpdatedMap.put("doc",updateMap);
        finalUpdatedMap.put("doc_as_upsert","true");
        client.prepareIndex().setId(id).setSource(finalUpdatedMap).setIndex(index).setType(type).execute(
            new ActionListener<IndexResponse>() {
                @Override public void onResponse(IndexResponse indexResponse) {

                }

                @Override public void onFailure(Exception e) {

                }
            });



    }

    /**
     *used to add document, if no id, then use build-in id generate way, if have just use it.
     * @param updateMap
     */
    @Override
    public void add(Map<String, String> updateMap) {
        checkParameters(updateMap);
        String id = (String) updateMap.get("id");
        String type = updateMap.get("type");
        String index = updateMap.get("index");
        client.prepareIndex().setId(id).setSource(updateMap).setIndex(index).setType(type).execute(
            new ActionListener<IndexResponse>() {
                @Override public void onResponse(IndexResponse indexResponse) {

                }

                @Override public void onFailure(Exception e) {

                }
            });


    }


    /**
     *
     * @param deletedMap
     */
    @Override
    public void delete(Map<String, String> deletedMap) {
        checkParameters(deletedMap);
        String id = (String) deletedMap.get("id");
        String type = deletedMap.get("type");
        String index = deletedMap.get("index");
        client.prepareDelete(index,type,id).execute(new ActionListener<DeleteResponse>() {
            @Override public void onResponse(DeleteResponse deleteResponse) {

            }

            @Override public void onFailure(Exception e) {

            }
        });



    }

    /**
     *
     * @param updatesMap
     * @return
     */
    public static String generateEsUpdateScriptFromMap(Map<String, String> updatesMap) {
        String id = updatesMap.get("id");
        updatesMap.remove("id");
        updatesMap.remove("index");
        updatesMap.remove("type");
        JsonObject jsonObj = new JsonObject();
        StringBuffer scriptBuffer = new StringBuffer();
        JsonObject jsonObject_1 = new JsonObject();
        for (Map.Entry<String, String> entrySet : updatesMap.entrySet()) {
            String key = entrySet.getKey();
            String value = entrySet.getValue();
            scriptBuffer.append("ctx._source.").append(key).append("=" + key + ";");
            jsonObject_1.addProperty(key, value);

        }
        jsonObj.addProperty("script", scriptBuffer.toString());

        jsonObj.add("params", jsonObject_1);
        //add upsert script
        //if document no existed, then create document by id given
        jsonObj.add("upsert", jsonObject_1);
        return new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create().toJson(jsonObj);

    }
    public static String generateEsUpdateScriptFromMapUsingFastJson(Map<String, String> updatesMap) {
        String id = updatesMap.get("id");
        updatesMap.remove("id");
        updatesMap.remove("index");
        updatesMap.remove("type");
        JsonObject jsonObj = new JsonObject();
        StringBuffer scriptBuffer = new StringBuffer();
        JsonObject jsonObject_1 = new JsonObject();
        for (Map.Entry<String, String> entrySet : updatesMap.entrySet()) {
            String key = entrySet.getKey();
            String value = entrySet.getValue();
            scriptBuffer.append("ctx._source.").append(key).append("=" + key + ";");
            jsonObject_1.addProperty(key, value);

        }
        jsonObj.addProperty("script", scriptBuffer.toString());

        jsonObj.add("params", jsonObject_1);
        //add upsert script
        //if document no existed, then create document by id given
        jsonObj.add("upsert", jsonObject_1);
        return new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create().toJson(jsonObj);

    }

    public static void main(String[] args) throws Exception {





////Object to JSON in String
//        JSONObject jsonObject=null;
//        String jsonInString = mapper.writeValueAsString(updatedMap);
//        System.out.print(jsonInString);
        String elasticSearchUrl="http://172.16.9.42:9200,http://172.16.9.42:9201,http://172.16.9.42:9202,http://172.16.9.43:9200,http://172.16.9.43:9201,http://172.16.9.43:9202,http://172.16.9.44:9200,http://172.16.9.44:9201,http://172.16.9.44:9202,http://172.16.9.45:9200,http://172.16.9.45:9201,http://172.16.9.45:9202,http://172.16.9.46:9200,http://172.16.9.46:9201,http://172.16.9.46:9202";
//        JestClient client = null;
//        Set<String> servers= Sets.newHashSet();
//        String[] servers_str=elasticSearchUrl.split(",");
//        for (String server_str:servers_str){
//            servers.add(server_str);
//        }
//
//
//        HttpClientConfig clientConfig = new HttpClientConfig.Builder(servers).multiThreaded(true)
//                .connTimeout(6*1000)
//                .readTimeout(6*1000)
//                .defaultMaxTotalConnectionPerRoute(1000)
//                .defaultCredentials("zhaoyufei","zhaoyufei")
//                .build();
//
//        JestClientFactory factory = new JestClientFactory();
//        factory.setHttpClientConfig(clientConfig);
//        client = factory.getObject();
//
//        DeleteByQuery deleteAllUserJohn = new DeleteByQuery.Builder("\"query\" : {\n" +
//                "        \n" +
//                "    \"range\" : {\n" +
//                "        \"accept_date\" : {\n" +
//                "            \"lt\" :  \"now-7d/d\",\n" +
//                "            \"gte\": \"now-8d/d\"\n" +
//                "        }\n" +
//                "    \n" +
//                "}\n" +
//                "    }")
//                .addIndex("baihe")
//                .addType("ha")
//                .build();
//        JestResult jr=client.execute(deleteAllUserJohn);
//       System.out.print(jr.getErrorMessage());





    }

}
