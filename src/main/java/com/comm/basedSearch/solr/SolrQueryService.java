/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.comm.basedSearch.solr;

import com.comm.basedSearch.entity.CommonQuery;
import com.comm.basedSearch.service.AbstractQueryService;
import com.comm.basedSearch.service.QueryGenerator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 *
 * @author jasstion
 */
public class SolrQueryService extends AbstractQueryService<CommonQuery> {
    protected final static Logger LOGGER = LoggerFactory.getLogger(SolrQueryService.class);

    @Override
    public List<Map<String, Object>> query(CommonQuery commonQuery) throws Exception {
        List<Map<String, Object>> results = Lists.newArrayList();

        int gender = commonQuery.getGender();
        if (commonQuery.getGender() < 0) {
            throw new RuntimeException("请正确的初始化参数，指定用户性别。");
        }
        CloudSolrServer cloudSolrServer = null;
//        if (gender == 0) {
//            cloudSolrServer = CloudSolr.getWomanInstance();
//        }
//        if (gender == 1) {
//            cloudSolrServer = CloudSolr.getManInstance();
//        }
        QueryGenerator<SolrQuery,CommonQuery> queryGenerator = new SolrQueryGenerator();
        SolrQuery solrQuery = queryGenerator.generateFinalQuery(commonQuery);
        LOGGER.debug("generted solr query:"+solrQuery.toString()+"");
        
        QueryResponse solrRespons = cloudSolrServer.query(solrQuery);
        //int totalCount=Integer.parseInt((String)solrRespons.getResponse().get("numFound"));
        
        SolrDocumentList solrResult = solrRespons.getResults();
        for (SolrDocument solrDocument : solrResult) {
            Map<String, Object> resultMap = Maps.newHashMap();
            List<String> flList = commonQuery.getFls();
            for (String fl : flList) {
                Object entry=solrDocument.get(fl);

                if(fl.equals("userID")){
                    String userID = String.valueOf(solrDocument.get(fl));
                    if (userID == null) {
                        continue;
                    }
                    userID = userID.replace(".", "").replaceAll("E[\\d]{0,}", "");
                    entry=userID;
                }
                resultMap.put(fl, entry);
            }
            results.add(resultMap);
        }

        return results;

    }

   

}
