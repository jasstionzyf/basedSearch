/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.comm.basedSearch.solr;

import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.comm.basedSearch.entity.QueryItem;
import com.comm.basedSearch.entity.SolrCommonQuery;
import com.comm.basedSearch.entity.SortItem;
import com.comm.basedSearch.entity.SubQuery;
import com.comm.basedSearch.service.QueryGenerator;
import com.comm.basedSearch.utils.Instances;
import com.google.common.collect.Lists;

/**
 *
 * @author jasstion
 */
public class SolrQueryGenerator implements QueryGenerator<SolrQuery, SolrCommonQuery> {

    protected final static Logger LOGGER = LoggerFactory.getLogger(SolrQueryGenerator.class);

    @Override
    public SolrQuery generateFinalQuery(SolrCommonQuery query_) {
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setParam("collection",query_.getCollectionName());
        SubQuery query=query_.getSubQuery();
        if (query == null) {
            return solrQuery;
        }
        StringBuffer solrQueryBuffer = new StringBuffer();
        makeFinalSolrQuery(query, solrQueryBuffer);


        int pageNum = query_.getPageNum();
        if (pageNum < 1) {
            //default first page
            pageNum = 1;
        } else {
            pageNum = query_.getPageNum();
        }
        List<SortItem> sortItems = query_.getSortItems();
        if (sortItems == null) {
            sortItems = Lists.newArrayList();
        }
        for (String functionQueryString : query_.getFunctionQuerysList()) {

            solrQueryBuffer.append("_query_:\"{!func}" + functionQueryString + "\"" + " AND ");

        }
        if (solrQueryBuffer.toString().contains("AND")) {

            int and = solrQueryBuffer.lastIndexOf("AND");
            solrQueryBuffer.replace(and, and + 3, "");
        }
        if (solrQueryBuffer.toString().trim().length() == 0) {
            solrQueryBuffer.append("*:*");
        }
        for (SortItem sortItem : sortItems) {
            String fieldName = sortItem.getFieldName();
            String order = sortItem.getSort();

            if (order == null) {
                LOGGER.info("ingore sort fieldName:" + fieldName + ", please configure it.");
                continue;
            }
            if (order.trim().equals("asc")) {
                solrQuery.setSort(fieldName, SolrQuery.ORDER.asc);
            }
            if (order.trim().equals("desc")) {
                solrQuery.addSort(fieldName, SolrQuery.ORDER.desc);

            }

        }

        for (String fl : query_.getFls()) {
            solrQuery.addField(fl);

        }
        int pageSize = query_.getPageSize();
        solrQuery.add("start", String.valueOf((pageNum - 1) * pageSize));
        solrQuery.add("rows", String.valueOf(pageSize));

        //if have distance query
        //fq=_query_:%22{!geofilt}%22&sfield=location&pt=45.15,-93.85&d=50000&sort=geodist()%20asc&fl=score,geodist(),location
        String location = query_.getLocationPoint();
        Double distance = query_.getDistance();

        if (location != null && distance != null) {
            solrQuery.add("d", distance.toString());
            solrQuery.add("pt", location);
            solrQuery.add("sfield", "location");
            solrQuery.add("fq", "_query_:{!geofilt}");
            solrQuery.addSort("geodist()", SolrQuery.ORDER.asc);

        }
        LOGGER.info(solrQueryBuffer.toString());
        solrQuery.setQuery(solrQueryBuffer.toString());

        return solrQuery;
    }

    private void makeFinalSolrQuery(SubQuery query, final StringBuffer solrQueryBuffer) {
        QueryItem queryItem=query.getQueryItem();
        String logic = query.getLogic();
        List<SubQuery> subQuerys = query.getSubQuerys();

        if (subQuerys!=null&&!subQuerys.isEmpty()) {
            solrQueryBuffer.append("( ");
            int length = subQuerys.size();
            for (int i = 0; i < subQuerys.size(); i++) {
                makeFinalSolrQuery(subQuerys.get(i), solrQueryBuffer);
                if (i < (length - 1)) {
                    solrQueryBuffer.append(" " + logic + " ");
                }

            }

            solrQueryBuffer.append(" )");

        } else {
            if(queryItem!=null){
                solrQueryBuffer.append(generateQueryStrFromQueryItem(queryItem.getFieldName(), queryItem.getMatchedValues()));

            }
        }

        //finally 
    }

    private String generateQueryStrFromQueryItem(String fieldName, List<String> matchedValues) {
        StringBuilder queryStr = new StringBuilder();

        boolean ifNotEqual = false;
        if (fieldName.contains("--")) {
            ifNotEqual = true;
            fieldName = fieldName.replace("--", "");

        }
        if (ifNotEqual) {
            queryStr.append("-");
        }
        queryStr.append("( ");

        if (matchedValues.size() > 0) {
            for (String matchValue : matchedValues) {
                if (matchValue == null || matchValue.trim().length() < 1) {
                    continue;
                }

                if (matchValue.contains("TO")) {
                    String[] vs = matchValue.split("#");
                    if (vs.length < 3) {
                        continue;
                    }
                    String bv = vs[0];
                    String ev = vs[2];

                    queryStr.append(fieldName + " : " + "[" + bv + " TO " + ev + "]" + " OR ");

                } else {

                    queryStr.append(fieldName + " : " + matchValue + " OR ");

                }

            }
            if (queryStr.toString().contains("OR")) {
                int or = queryStr.lastIndexOf("OR");
                queryStr.replace(or, or + 2, "");
            }

        }
        queryStr.append(" )");
        if (queryStr.length() < 5) {
            return null;
        }

        return queryStr.toString();

    }

    public static void main(String[] args) {
        SubQuery ageQuery = new SubQuery();
        QueryItem ageQueryItem=new QueryItem();
        ageQueryItem.setFieldName("age");
        ageQueryItem.setMatchedValues(Lists.newArrayList("1988"));
        ageQuery.setQueryItem(ageQueryItem);
        SubQuery heightQuery = new SubQuery();
        QueryItem heightQueryItem=new QueryItem();

        heightQueryItem.setFieldName("height");
        heightQueryItem.setMatchedValues(Lists.newArrayList("180"));
        heightQuery.setQueryItem(heightQueryItem);
        SubQuery finalQuery = new SubQuery();
        finalQuery.setLogic("OR");
        List<SubQuery> subQueries=Lists.newArrayList();
        finalQuery.setSubQuerys(subQueries);
        finalQuery.getSubQuerys().add(ageQuery);
        finalQuery.getSubQuerys().add(heightQuery);
        SubQuery sexQuery = new SubQuery();
        QueryItem sexQueryItem=new QueryItem();

        sexQueryItem.setFieldName("sex");
        sexQueryItem.setMatchedValues(Lists.newArrayList("1"));
        sexQuery.setQueryItem(sexQueryItem);

        SubQuery finalQuery_ = new SubQuery();
        finalQuery_.setLogic("AND");
        List<SubQuery> subQueries_=Lists.newArrayList();
        finalQuery_.setSubQuerys(subQueries_);
        finalQuery_.getSubQuerys().add(finalQuery);
        finalQuery_.getSubQuerys().add(sexQuery);
        LOGGER.info(Instances.gson.toJson(finalQuery_) + "\n");
        SolrCommonQuery solrCommonQuery=new SolrCommonQuery("user");
        solrCommonQuery.setSubQuery(finalQuery_);
        SolrQuery solrQuery=new SolrQueryGenerator().generateFinalQuery(solrCommonQuery);
        LOGGER.info(solrQuery.toString());

    }
}
