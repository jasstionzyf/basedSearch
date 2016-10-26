package com.comm.basedSearch.utils;

import com.comm.basedSearch.entity.EsCommonQuery;
import com.comm.basedSearch.entity.QueryItem;
import com.comm.basedSearch.entity.SortItem;
import com.comm.basedSearch.entity.SubQuery;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jasstion on 24/10/2016.
 */
public class GsonHelper {
    private final static GsonBuilder g = new GsonBuilder();

    private final static Gson gson = g.create();

    public static String objToJson(Object obj){
        String jsonStr=null;
        jsonStr=gson.toJson(obj);

        return jsonStr;


    }
    public static Object jsonToObj(String jsonStr,Class aclass){
        Object obj=null;
        Gson gson = new Gson();
        JsonReader reader = new JsonReader(new StringReader(jsonStr));
        reader.setLenient(true);


        obj =Instances.gson.fromJson(jsonStr,aclass);
        return obj;

    }

    public static Object jsonToObj(String jsonStr,Type type){
        Object obj=null;
        Gson gson = new Gson();
        JsonReader reader = new JsonReader(new StringReader(jsonStr));
        reader.setLenient(true);


        obj =Instances.gson.fromJson(jsonStr,type);
        return obj;

    }



    public static void main(String[] args){
        String indexName="com";
        String typeName="user";

        List<QueryItem> items = Lists.newArrayList();
        QueryItem queryItem=new QueryItem("des",Lists.newArrayList("box"));
        queryItem.setIsPayload(true);
        items.add(queryItem);
        String listStr=GsonHelper.objToJson(items);
        System.out.print(listStr+"\n");
        List<QueryItem> items1= (List<QueryItem>) GsonHelper.jsonToObj(listStr,new TypeToken<ArrayList<QueryItem>>() {
        }.getType());
        System.out.print(items1.get(0).getMatchedValues()+"\n");
        System.out.print(GsonHelper.objToJson(items1)+"\n");

        SubQuery subQuery=new SubQuery();
        subQuery.setLogic("AND");
        List<SubQuery> subQueries=Lists.newArrayList(new SubQuery("AND", new QueryItem("des", Lists.newArrayList("basket", "football"))));
        QueryItem queryItem1=new QueryItem("des", Lists.newArrayList("boss"));
        queryItem1.setIsPayload(true);
        SubQuery subQuery1=new SubQuery("NOT",queryItem1 );
        subQuery1.setSubQuerys(Lists.newArrayList(new SubQuery("AND", new QueryItem("age", Lists.newArrayList("1220TO1230")))));
        subQueries.add(subQuery1);
        subQuery.setSubQuerys(subQueries);


        final List<String> fls = Lists.newArrayList("userId","des","name","age");

        List<SortItem> sortItems = Lists.newArrayList();
        //logstash-2015.12.10 log4j
        //EsCommonQuery baiheQuery = new EsCommonQuery(items, 1, 18, sortItems, fls, "baihe_user", "user");
        EsCommonQuery baiheQuery = new EsCommonQuery(items, 1, 5, sortItems, fls, indexName, typeName);
        baiheQuery.setSubQuery(subQuery);
        baiheQuery.setScoreScript("100*_score");

        String jsonStr=GsonHelper.objToJson(baiheQuery);
        System.out.print(jsonStr+"\n");

        EsCommonQuery esCommonQuery=(EsCommonQuery)GsonHelper.jsonToObj(jsonStr,EsCommonQuery.class);
        System.out.print(esCommonQuery.toString()+"\n");

        System.out.print(esCommonQuery.equals(baiheQuery));




    }
}
