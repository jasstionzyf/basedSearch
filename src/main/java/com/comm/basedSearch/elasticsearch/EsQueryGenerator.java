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
import com.comm.basedSearch.service.QueryGenerator;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.index.query.functionscore.script.ScriptScoreFunctionBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbib.elasticsearch.index.query.PayloadTermQueryBuilder;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 *
 * @author jasstion
 */
public class EsQueryGenerator implements QueryGenerator<EsQueryGenerator.EsQueryWrapper, EsCommonQuery> {

    protected final static Logger LOGGER = LoggerFactory.getLogger(EsQueryGenerator.class);

    @Override
    public EsQueryGenerator.EsQueryWrapper generateFinalQuery(EsCommonQuery query) {
        String index = query.getIndex();
        String type = query.getType();
        List<String> fls = query.getFls();
        List<QueryItem> queryItems = query.getQueryItems();
        List<SortItem> sortItems = query.getSortItems();
        int pageNum = query.getPageNum();
        int pageSize = query.getPageSize();
        if (pageNum < 1) {
            //default first page
            pageNum = 1;
        } else {
            pageNum = query.getPageNum();
        }

        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.from((pageNum - 1) * pageSize).size(pageSize);

        for (SortItem sortItem : sortItems) {
            searchSourceBuilder.sort(sortItem.getFieldName(), sortItem.getSort().trim().equals("asc") ? SortOrder.ASC : SortOrder.DESC);

        }


        String location = query.getLocationPoint();
        Double distance = query.getDistance();
        QueryBuilder distanceQueryBuilder=null;
        if (location != null && distance != null) {
            distanceQueryBuilder= new GeoDistanceQueryBuilder("location")
                    .geohash(location)
                    .distance(distance, DistanceUnit.KILOMETERS)
                    .optimizeBbox("memory")
                    .geoDistance(GeoDistance.ARC);



        }
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //解析queryItems
        for (QueryItem queryItem : queryItems) {

            BoolQueryBuilder tmpBoolQueryBuilder = new BoolQueryBuilder();
            makeBoolQuery(queryItem, tmpBoolQueryBuilder);

            boolQueryBuilder.must(tmpBoolQueryBuilder);

        }
        //解析subQuery
        SubQuery subQuery=query.getSubQuery();
        if(subQuery!=null){
            makeFinalBoolQuery(subQuery,boolQueryBuilder);


        }


        //deal with function score query
//        if(StringUtils.isNotEmpty(query.getFunctionScoreQuery())){
//
//
//        }


        if(distanceQueryBuilder!=null){
            boolQueryBuilder.must(distanceQueryBuilder);

        }
        //deal with score script
        FunctionScoreQueryBuilder functionScoreQueryBuilder=null;
        if(!StringUtils.isEmpty(query.getScoreScript())){

           // String inlineScript = "((_score+1)*1.0 + 2.0/((1.469097882-(doc['uploadTime'].value as double)/1000000000)*17000+1.0))/_score";
            String inlineScript=query.getScoreScript();
            Map<String, Object> params = Maps.newHashMap();
            String language="javascript";


            Script script = new Script(inlineScript, ScriptService.ScriptType.INLINE, language, params);
            ScriptScoreFunctionBuilder scriptBuilder = ScoreFunctionBuilders.scriptFunction(script);


            functionScoreQueryBuilder = new FunctionScoreQueryBuilder(boolQueryBuilder);
            functionScoreQueryBuilder.add(scriptBuilder);


        }
        if(functionScoreQueryBuilder!=null){
            searchSourceBuilder.query(functionScoreQueryBuilder);

        }
        else{
            searchSourceBuilder.query(boolQueryBuilder);

        }
        //apply source field filter
        //searchSourceBuilder.fields(fls);
        String[] includeFields = new String[fls.size()];
        int i = 0;
        for (String includeField : fls) {
            includeFields[i++] = includeField;
        }
        searchSourceBuilder.fetchSource(includeFields, null);

        String finalQuery = searchSourceBuilder.toString();
        LOGGER.debug(finalQuery);
        EsQueryWrapper esQueryWrapper=new EsQueryWrapper(index,searchSourceBuilder,type);
        return esQueryWrapper;
    }

    private void makeBoolQuery(QueryItem queryItem, BoolQueryBuilder tmpBoolQueryBuilder) {
        String fieldName = queryItem.getFieldName();
        List<String> matchValues = queryItem.getMatchedValues();
        if (matchValues.size() > 0) {
            for (String matchValue : matchValues) {
                if (matchValue == null || matchValue.trim().length() < 1) {
                    continue;
                }

                if (matchValue.contains("TO")) {
                    String bv = null;
                    String ev = null;
                    if (matchValue.contains("#")) {
                        String[] vs = matchValue.split("#");
                        if (vs.length < 3) {
                            continue;
                        }
                        bv = vs[0];
                        ev = vs[2];
                    } else {
                        String[] vs = matchValue.split("TO");
                        if (vs.length < 2) {
                            continue;
                        }
                        bv = vs[0];
                        ev = vs[1];
                    }
//                        String[] vs = matchValue.split("#");
//                        if (vs.length < 3) {
//                            continue;
//                        }
//                        String bv = vs[0];
//                        String ev = vs[2];
                    RangeQueryBuilder termQueryBuilder = new RangeQueryBuilder(fieldName);
                    //range query date query or number query
                    //2014-02-15T18:59:51Z
                    if (bv.contains("T")) {
                        //date range query

                        termQueryBuilder.format("yyyy-MM-dd'T'HH:mm:ss'Z'").gte(bv).lte(ev);
                        tmpBoolQueryBuilder.should(termQueryBuilder);

                    } else {
                        termQueryBuilder.gte(bv).lte(ev);

                    }
                    tmpBoolQueryBuilder.should(termQueryBuilder);

                } else {

                    //not range query
                    //if payload term query
                    if(queryItem.isPayload()){
                        QueryBuilder termQueryBuilder = new PayloadTermQueryBuilder(fieldName, matchValue);
                        tmpBoolQueryBuilder.should(termQueryBuilder);


                    }
                    else{
                        TermQueryBuilder termQueryBuilder = new TermQueryBuilder(fieldName, matchValue);
                        tmpBoolQueryBuilder.should(termQueryBuilder);

                    }


                }

            }

        }
    }

    public static class  EsQueryWrapper implements Serializable{
    private SearchSourceBuilder searchSourceBuilder=null;
    private String indexName=null;
    private String typeName=null;

    public EsQueryWrapper(String indexName, SearchSourceBuilder searchSourceBuilder, String typeName) {
        this.indexName = indexName;
        this.searchSourceBuilder = searchSourceBuilder;
        this.typeName = typeName;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public SearchSourceBuilder getSearchSourceBuilder() {
        return searchSourceBuilder;
    }

    public void setSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder) {
        this.searchSourceBuilder = searchSourceBuilder;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }
}
    public static void main(String[] args){
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();



    }
    private void makeFinalBoolQuery(SubQuery query, final BoolQueryBuilder boolQueryBuilder) {
        QueryItem queryItem=query.getQueryItem();
        List<SubQuery> subQuerys = query.getSubQuerys();


        if (!subQuerys.isEmpty()) {
            int length = subQuerys.size();
           //
          //  tempBoolQueryBuilder=new BoolQueryBuilder();
            makeBoolQuery(queryItem, boolQueryBuilder);
            //boolQueryBuilder.must(tempBoolQueryBuilder);
            BoolQueryBuilder tempBoolQueryBuilder=new BoolQueryBuilder();

            for (int i = 0; i < subQuerys.size(); i++) {


                    makeFinalBoolQuery(subQuerys.get(i),tempBoolQueryBuilder);


            }
            if(query.getLogic().equals("AND")){
                boolQueryBuilder.must(tempBoolQueryBuilder);

            }
            if(query.getLogic().equals("OR")){
                boolQueryBuilder.should(tempBoolQueryBuilder);

            }
            if(query.getLogic().equals("NOT")){
                boolQueryBuilder.mustNot(tempBoolQueryBuilder);

            }


        } else {
           // BoolQueryBuilder tempBoolQueryBuilder=new BoolQueryBuilder();
            makeBoolQuery(queryItem, boolQueryBuilder);

           // boolQueryBuilder.must(tempBoolQueryBuilder);





        }

        //finally
    }
}
