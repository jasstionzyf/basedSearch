/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.comm.basedSearch.elasticsearch;

import com.comm.basedSearch.entity.EsCommonQuery;
import com.comm.basedSearch.service.AbstractQueryService;
import com.google.common.collect.Lists;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

/**
 *
 * @author jasstion
 */
public class EsQueryService extends AbstractQueryService<EsCommonQuery> {
    final static ResourceBundle settings = ResourceBundle.getBundle("basedSearch");


    private TransportClient client=null;


    public EsQueryService() {
        super();


        String hosts=settings.getString("elasticSearchHosts");
         client = new PreBuiltTransportClient(Settings.EMPTY);

        //client=TransportClient.builder().build();

        for(String host:hosts.split(";")){

            try {
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host.split(":")[0]), Integer.parseInt(host.split(":")[1])));
            } catch (Exception e) {
                LOGGER.error("error to get elasticsearch client!");
            }

        }




    }

    public EsQueryService(String elasticSearchHosts,String clusterName) {
        super();

    Settings settings = Settings.builder().put("cluster.name", clusterName).build();

    client = new PreBuiltTransportClient(settings);

    for(String host:elasticSearchHosts.split(";")){

        try {
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host.split(":")[0]), Integer.parseInt(host.split(":")[1])));
        } catch (Exception e) {
            LOGGER.error("error to get elasticsearch client!");
        }

    }




}
    public EsQueryService(String elasticSearchHosts) {
        super();


        String hosts=settings.getString("elasticSearchHosts");
        client = new PreBuiltTransportClient(Settings.EMPTY);

        for(String host:elasticSearchHosts.split(";")){

            try {
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host.split(":")[0]), Integer.parseInt(host.split(":")[1])));
            } catch (Exception e) {
                LOGGER.error("error to get elasticsearch client!");
            }

        }




    }
    @Override
    protected void finalize() throws Throwable {
        super.finalize();

    }



    @Override
    public List<Map<String, Object>> query(EsCommonQuery baiheQuery) throws Exception {
        List<Map<String, Object>> results = Lists.newArrayList();
        EsQueryGenerator.EsQueryWrapper  esQueryWrapper=new EsQueryGenerator().generateFinalQuery(baiheQuery);
        SearchResponse searchResponse=client.prepareSearch().setQuery(esQueryWrapper.getSearchSourceBuilder().query()).setIndices(esQueryWrapper.getIndexName())
            .execute().actionGet();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            Map<String, Object> values = hit.getSource();
            values.put("score",hit.getScore());



            results.add(values);

        }






        return results;

    }


}
