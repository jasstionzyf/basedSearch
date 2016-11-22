/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.comm.basedSearch.solr;

import com.comm.basedSearch.service.UpdateService;
import com.google.common.collect.Maps;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author jasstion
 */
public class SolrUpdateService implements UpdateService {

    final static ResourceBundle solrProperties = ResourceBundle.getBundle("basedSearch");
    final static Map<String, CloudSolrServer> serverMap = Maps.newHashMap();
    protected CloudSolrServer cloudSolrServer = null;


    /**
     *
     * @throws MalformedURLException
     */
    public SolrUpdateService(Properties settings) throws MalformedURLException {
        super();
        //load solr related properties
        try {
            String zkHost = settings.getProperty("solrcloud.zkHost");
            int max_connections = Integer.parseInt(settings.getProperty("solrcloud.max_connections"));
            int max_connections_per_host =
                Integer.parseInt(settings.getProperty("solrcloud.max_connections_per_host"));
            int zkConnectTimeout = Integer.parseInt(settings.getProperty("solrcloud.zkConnectTimeout"));
            int zkClientTimeout = Integer.parseInt(settings.getProperty("solrcloud.zkClientTimeout"));

            ModifiableSolrParams params = new ModifiableSolrParams();
            params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, max_connections);
            params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, max_connections_per_host);
            HttpClient client = HttpClientUtil.createClient(params);

            LBHttpSolrServer lbServer = new LBHttpSolrServer(client);

            cloudSolrServer = new CloudSolrServer(zkHost, lbServer);
            cloudSolrServer.setZkConnectTimeout(zkConnectTimeout);
            cloudSolrServer.setZkClientTimeout(zkClientTimeout);
        } catch (Exception e) {

        }
    }




    /**
     *
     * @param updateMap make sure dateformat: yyyy-MM-ddTHH:mm:ssZ
     *
     * make sure updateMap contains collectionName of solr you want to update if
     * unique key fieldName==id, then contains id value, if not, then put the
     * unique key fieldName as idKey, at same time contains unique key value
     *
     */
    @Override

    public void update(Map<String, String> updateMap) {
        String collectionName = updateMap.get("collectionName");
        updateMap.remove("collectionName");
        if (collectionName == null) {
            throw new IllegalArgumentException("please set your solr collectionName");
        }

        CloudSolrServer cloudSolrServer = serverMap.get(collectionName);
        SolrInputDocument document = new SolrInputDocument();
        String idKey = updateMap.get("idKey");
        String id = updateMap.get("id");
        if (id == null&&idKey == null) {
            return;
        }
        if (idKey == null) {
            document.addField("id", updateMap.get("id"));
            updateMap.remove("id");
        }
        if (id == null) {
            document.addField(idKey, updateMap.get(idKey));
            updateMap.remove(idKey);

        }

        for (Map.Entry<String, String> entrySet : updateMap.entrySet()) {
            String key = entrySet.getKey();
            String value = entrySet.getValue();

            Map<String, Object> setValueMap = Maps.newHashMapWithExpectedSize(1);
            setValueMap.put("set", value);
            document.addField(key, setValueMap);

        }
        try {
            cloudSolrServer.add(document);
            //cloudSolrServer.commit();
        } catch (SolrServerException ex) {
            Logger.getLogger(SolrUpdateService.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(SolrUpdateService.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    /**
     *
     * @param updateMap
     */
    @Override
    public void add(Map<String, String> updateMap) {
        String collectionName = updateMap.get("collectionName");
        updateMap.remove("collectionName");
        if (collectionName == null) {
            throw new IllegalArgumentException("please set your solr collectionName");
        }
        CloudSolrServer cloudSolrServer = serverMap.get(collectionName);
        SolrInputDocument document = new SolrInputDocument();
        for (Map.Entry<String, String> entrySet : updateMap.entrySet()) {
            String key = entrySet.getKey();
            String value = entrySet.getValue();

            document.addField(key, value);

        }
        try {
            cloudSolrServer.add(document);
            //cloudSolrServer.commit();
        } catch (SolrServerException ex) {
            Logger.getLogger(SolrUpdateService.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(SolrUpdateService.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     *
     * @param updateMap
     */
    @Override
    public void delete(Map<String, String> updateMap) {
        String collectionName = updateMap.get("collectionName");
        if (collectionName == null) {
            throw new IllegalArgumentException("please set your solr collectionName");
        }
        String id = updateMap.get("id");
        CloudSolrServer cloudSolrServer = serverMap.get(collectionName);
        try {
            cloudSolrServer.deleteById(id);
            cloudSolrServer.commit();
        } catch (SolrServerException ex) {
            Logger.getLogger(SolrUpdateService.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(SolrUpdateService.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override public void bulkUpdate(List<Map<String, String>> updatedMaps) throws Exception {

    }

    @Override public void bulkAdd(List<Map<String, String>> updatedMaps) throws Exception {

    }

}
