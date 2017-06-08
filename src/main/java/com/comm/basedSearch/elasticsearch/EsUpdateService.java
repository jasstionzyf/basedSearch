/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.comm.basedSearch.elasticsearch;

import com.comm.basedSearch.service.UpdateService;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.*;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author jasstion
 */
public class EsUpdateService implements UpdateService {

//    final static ResourceBundle solrProperties = ResourceBundle.getBundle("elasticSearch");
    protected final static org.slf4j.Logger mLog = LoggerFactory.getLogger(EsUpdateService.class);


    /**
     *
     */
    private static JestClient client = null;
    private static void populateClient(String elasticSearchUrl) {
        if (client == null) {
            Set<String> servers= Sets.newHashSet();
            String[] servers_str=elasticSearchUrl.split(",");
            for (String server_str:servers_str){
                servers.add(server_str);
            }


            HttpClientConfig clientConfig = new HttpClientConfig.Builder(servers).multiThreaded(true)
                    .connTimeout(6*1000)
                    .readTimeout(100*1000)
                    .defaultMaxTotalConnectionPerRoute(1000)
                    .build();

            JestClientFactory factory = new JestClientFactory();
            factory.setHttpClientConfig(clientConfig);
            client = factory.getObject();
        }

    }

//    /**
//     *
//     */
//    public EsUpdateService() {
//        String elasticSearchUrl = solrProperties.getString("elasticSearchHosts");
//        populateClient(elasticSearchUrl);
//
//    }

    public EsUpdateService(String elasticSearchUrl){
           populateClient(elasticSearchUrl);

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


        Bulk.Builder builder = new Bulk.Builder();
        List<Update> updateList =Lists.newArrayList();

        for(Map<String, String> updateMap:updatedMaps){
            String id = (String) updateMap.get("id");
            String type = updateMap.get("type");
            String index = updateMap.get("index");
            String routingField=updateMap.get("routingField");
            updateMap.remove("routingField");

           if(id==null || type==null || index==null){
               continue;

           }

          //  String updateScript = generateEsUpdateScriptFromMap(updateMap);
           // Update update = new Update.Builder(updateScript+"\n").index(index).type(type).id(id).build();
            Map<String,Object> finalUpdatedMap=Maps.newHashMap();
            finalUpdatedMap.put("doc",updateMap);
            finalUpdatedMap.put("doc_as_upsert","true");
            Update update = new Update.Builder(finalUpdatedMap).index(index).type(type).id(id).setParameter("routing",updateMap.get(routingField)).build();
            updateList.add(update);

        }



        Bulk bulk = new Bulk.Builder()

                .addAction(updateList)
                .build();
       // System.out.print(bulk.getURI());



            JestResult jestResult=client.execute(bulk);
            processJestResult(jestResult);




    }


    /**
     *  recommended to use batchUpdated
     * used to batch add documents,do not care whether the input document has id field
     * @param updatedMaps
     */
    @Deprecated
    public void bulkAdd(List<Map<String, String>> updatedMaps)throws Exception{
        Bulk.Builder builder = new Bulk.Builder();
        List<Index> updateList =Lists.newArrayList();

        for(Map<String, String> updateMap:updatedMaps){
            String id = (String) updateMap.get("id");
            String type = updateMap.get("type");
            String index = updateMap.get("index");

            //
//            updateMap.remove("index");
//            updateMap.remove("type");
            Index indexAction=null;
            if(id!=null){
               // updateMap.remove("id");
                indexAction=new Index.Builder(updateMap).index(index).type(type).id(id).build();
            }
            else{
                indexAction= new Index.Builder(updateMap).index(index).type(type).build();
            }
            updateList.add(indexAction);

        }



        Bulk bulk = new Bulk.Builder()

                .addAction(updateList)
                .build();
        // System.out.print(bulk.getURI());


            JestResult jestResult=client.execute(bulk);
            processJestResult(jestResult);




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
        Update update = new Update.Builder(finalUpdatedMap).index(index).type(type).id(id).build();

        try {
            JestResult jestResult=client.execute(update);
            processJestResult(jestResult);

        } catch (IOException ex) {
            Logger.getLogger(EsUpdateService.class.getName()).log(Level.SEVERE, null, ex);
        }

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
        String routingField=updateMap.get("routingField");
        updateMap.remove("routingField");

       //
//        updateMap.remove("index");
//        updateMap.remove("type");
        Index indexAction=null;
        if(id!=null){
           // updateMap.remove("id");
            indexAction=new Index.Builder(updateMap).index(index).type(type).id(id).build();
        }
        else{
            indexAction= new Index.Builder(updateMap).index(index).type(type).setParameter("routing",updateMap.get(routingField)).build();
        }

        try {
          JestResult jestResult= client.execute(indexAction);
            processJestResult(jestResult);

        } catch (IOException ex) {
            throw new RuntimeException("新增数据出错，错误信息："+ex.getMessage()+"");
        }
    }
    private void processJestResult(JestResult jestResult){
        if(!jestResult.isSucceeded()){
            throw new RuntimeException("数据操作出错，错误信息："+jestResult.getJsonString()+"");

        }
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
        Delete deleteAction = new Delete.Builder(id).index(index).type(type).build();
        try {
            JestResult jestResult= client.execute(deleteAction);
            processJestResult(jestResult);

        } catch (IOException ex) {
            throw new RuntimeException("删除数据出错，错误信息："+ex.getMessage()+"");
        }

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


        //test route function
        EsUpdateService esUpdateService=new EsUpdateService("http://127.0.0.1:9201");
        List<Map<String, String>> updatedMaps= Lists.newArrayList();
        Map<String, String> updatedMap = Maps.newHashMap();
        //updatedMap.put("nickname", "说好不哭11");
        updatedMap.put("index","vcg_image");




        updatedMap.put("type", "image");
        updatedMap.put("groupId", "13987");
        updatedMap.put("id","1111111111013");
        updatedMap.put("routingField","groupId");

        Map<String, String> updatedMap1 = Maps.newHashMap();
        updatedMap1.put("id","11111111168191113");
        updatedMap1.put("index","vcg_image");
        updatedMap1.put("routingField","groupId");




        updatedMap1.put("type", "image");
        updatedMap1.put("groupId", "2009813");

        Map<String, String> updatedMap2 = Maps.newHashMap();
        updatedMap2.put("id","1111111117822313");
        updatedMap2.put("index","vcg_image");

        updatedMap2.put("routingField","groupId");



        updatedMap2.put("type", "image");
        updatedMap2.put("groupId", "2000");
        updatedMaps.add(updatedMap);
        updatedMaps.add(updatedMap1);
        updatedMaps.add(updatedMap2);

//        esUpdateService.update(updatedMap);
        //  esUpdateService.bulkAdd(updatedMaps);
         esUpdateService.bulkUpdate(updatedMaps);


    }

}
