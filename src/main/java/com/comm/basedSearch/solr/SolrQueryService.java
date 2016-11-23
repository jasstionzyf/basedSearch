/*
 * To change this license header, choose License Headers in Project Properties. To change this
 * template file, choose Tools | Templates and open the template in the editor.
 */
package com.comm.basedSearch.solr;

import com.comm.basedSearch.entity.SolrCommonQuery;
import com.comm.basedSearch.service.AbstractQueryService;
import com.comm.basedSearch.service.QueryGenerator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author jasstion
 */
public class SolrQueryService extends AbstractQueryService<SolrCommonQuery> {
  protected CloudSolrServer cloudSolrServer = null;

  public SolrQueryService(Properties settings) {
    super();
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

  @Override
  public List<Map<String, Object>> query(SolrCommonQuery query) throws Exception {
    List<Map<String, Object>> results = Lists.newArrayList();

    QueryGenerator<SolrQuery, SolrCommonQuery> queryGenerator = new SolrQueryGenerator();
    SolrQuery solrQuery = queryGenerator.generateFinalQuery(query);
    LOGGER.debug("generted solr query:" + solrQuery.toString() + "");

    QueryResponse solrRespons = cloudSolrServer.query(solrQuery);
    // int totalCount=Integer.parseInt((String)solrRespons.getResponse().get("numFound"));

    SolrDocumentList solrResult = solrRespons.getResults();
    for (SolrDocument solrDocument : solrResult) {
      Map<String, Object> resultMap = Maps.newHashMap();
      List<String> flList = query.getFls();
      for (String fl : flList) {
        Object entry = solrDocument.get(fl);

        resultMap.put(fl, entry);
      }
      results.add(resultMap);
    }

    return results;
  }

}
