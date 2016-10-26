/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.comm.basedSearch.elasticsearch;

import com.comm.basedSearch.entity.EsCommonQuery;
import com.comm.basedSearch.service.AbstractQueryService;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.time.StopWatch;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

/**
 *
 * @author jasstion
 */
public class EsQueryService extends AbstractQueryService<EsCommonQuery> {

    final static ResourceBundle solrProperties = ResourceBundle.getBundle("elasticSearch");
    protected final static Logger LOGGER = LoggerFactory.getLogger(EsQueryService.class);
    private Client client=null;


    public EsQueryService(Client client) {
        super();
        if(client!=null){
            this.client=client;
        }
        else{
            String hosts=solrProperties.getString("elasticSearchHosts");
            TransportClient transportClient=TransportClient.builder().build();
            for(String host:hosts.split(";")){

                try {
                    transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host.split(":")[0]), Integer.parseInt(host.split(":")[1])));
                } catch (Exception e) {
                    LOGGER.error("error to get elasticsearch client!");
                }

            }
        }



    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        client.close();
    }

    @Override
    public List<Map<String, Object>> processQuery(EsCommonQuery baiheQuery) throws Exception {
        long timeTaken = 0;
        boolean cacheHit = false;
        List<Map<String, Object>> queryResult = null;
        StopWatch stopWatch = new StopWatch();

        // Start the watch, do some task and stop the watch.
        stopWatch.start();

        queryResult = this.queryCache(baiheQuery);
        if (queryResult != null && queryResult.size() > 1) {
            cacheHit = true;
            AbstractQueryService.QueryStatistics queryStatistics = new QueryStatistics(timeTaken, cacheHit);
            stopWatch.stop();
            timeTaken = stopWatch.getTime();
            this.recordQueryLog(queryStatistics);
            return queryResult;
        }
        cacheHit = false;
        queryResult = this.query(baiheQuery);
        stopWatch.stop();
        timeTaken = stopWatch.getTime();

        AbstractQueryService.QueryStatistics queryStatistics = new QueryStatistics(timeTaken, cacheHit);
        this.recordQueryLog(queryStatistics);
        this.cacheQueryResult(baiheQuery, queryResult);

        return queryResult;
    }

    @Override
    public List<Map<String, Object>> query(EsCommonQuery baiheQuery) throws Exception {
        List<Map<String, Object>> results = Lists.newArrayList();
        EsQueryGenerator.EsQueryWrapper  esQueryWrapper=new EsQueryGenerator().generateFinalQuery(baiheQuery);
        SearchResponse searchResponse=client.prepareSearch().setSource(esQueryWrapper.getSearchSourceBuilder().toString()).setIndices(esQueryWrapper.getIndexName())
                .execute().actionGet();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            Map<String, Object> values = hit.getSource();
            values.put("score",hit.getScore());



            results.add(values);

        }






        return results;

    }

}
