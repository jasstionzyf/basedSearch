/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.comm.basedSearch.solr;

import com.comm.basedSearch.entity.QueryItem;
import com.comm.basedSearch.entity.SubQuery;
import com.comm.basedSearch.service.QueryGenerator;
import org.apache.solr.client.solrj.SolrQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 *
 * @author jasstion
 */
public class AdvancedSolrQueryGenerator implements QueryGenerator<SolrQuery, SubQuery> {

    protected final static Logger LOGGER = LoggerFactory.getLogger(AdvancedSolrQueryGenerator.class);

    @Override
    public SolrQuery generateFinalQuery(SubQuery query) {
        SolrQuery solrQuery = new SolrQuery();
        if (query == null) {
            return solrQuery;
        }
        StringBuffer solrQueryBuffer = new StringBuffer();
        makeFinalSolrQuery(query, solrQueryBuffer);
        LOGGER.info(solrQueryBuffer.toString());
        return solrQuery;
    }

    private void makeFinalSolrQuery(SubQuery query, final StringBuffer solrQueryBuffer) {
        QueryItem queryItem=query.getQueryItem();
        String logic = query.getLogic();
        List<SubQuery> subQuerys = query.getSubQuerys();

        if (!subQuerys.isEmpty()) {
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
            solrQueryBuffer.append(generateQueryStrFromQueryItem(queryItem.getFieldName(), queryItem.getMatchedValues()));
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
//        SubQuery ageQuery = new SubQuery();
//        ageQuery.setFieldName("age");
//        ageQuery.setMatchedValues(Lists.newArrayList("1988"));
//        SubQuery heightQuery = new SubQuery();
//        heightQuery.setFieldName("height");
//        heightQuery.setMatchedValues(Lists.newArrayList("180"));
//        SubQuery finalQuery = new SubQuery();
//        finalQuery.setLogic("OR");
//        finalQuery.getSubQuerys().add(ageQuery);
//        finalQuery.getSubQuerys().add(heightQuery);
//        SubQuery sexQuery = new SubQuery();
//        sexQuery.setFieldName("sex");
//        sexQuery.setMatchedValues(Lists.newArrayList("1"));
//
//        SubQuery finalQuery_ = new SubQuery();
//        finalQuery_.setLogic("AND");
//        finalQuery_.getSubQuerys().add(finalQuery);
//        finalQuery_.getSubQuerys().add(sexQuery);
//        LOGGER.info(Instances.gson.toJson(finalQuery_) + "\n");
//        new AdvancedSolrQueryGenerator().generateFinalQuery(finalQuery_);

    }
}
