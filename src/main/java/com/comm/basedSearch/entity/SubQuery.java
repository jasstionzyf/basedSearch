/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.comm.basedSearch.entity;

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.List;

/**
 *
 * @author jasstion
 */
public class SubQuery implements Serializable {

    //AND or OR or NOT
    private String logic = "AND";
    private List<SubQuery> subQuerys = Lists.newArrayList();
    private QueryItem queryItem=new QueryItem();

    public QueryItem getQueryItem() {
        return queryItem;
    }

    public void setQueryItem(QueryItem queryItem) {
        this.queryItem = queryItem;
    }

    public SubQuery() {
        super();
    }

    public SubQuery(String logic, QueryItem queryItem, List<SubQuery> subQuerys) {

        this.logic = logic;
        this.queryItem = queryItem;
        this.subQuerys = subQuerys;
    }

    public SubQuery(String logic, QueryItem queryItem) {
        this.logic = logic;
        this.queryItem = queryItem;
    }

    public String getLogic() {
        return logic;
    }

    public void setLogic(String logic) {
        this.logic = logic;
    }

    public List<SubQuery> getSubQuerys() {
        return subQuerys;
    }

    public void setSubQuerys(List<SubQuery> subQuerys) {
        this.subQuerys = subQuerys;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SubQuery subQuery = (SubQuery) o;

        if (logic != null ? !logic.equals(subQuery.logic) : subQuery.logic != null) return false;
        if (subQuerys != null ? !subQuerys.equals(subQuery.subQuerys) : subQuery.subQuerys != null) return false;
        return !(queryItem != null ? !queryItem.equals(subQuery.queryItem) : subQuery.queryItem != null);

    }

    @Override
    public int hashCode() {
        int result = logic != null ? logic.hashCode() : 0;
        result = 31 * result + (subQuerys != null ? subQuerys.hashCode() : 0);
        result = 31 * result + (queryItem != null ? queryItem.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SubQuery{" +
                "logic='" + logic + '\'' +
                ", subQuerys=" + subQuerys +
                ", queryItem=" + queryItem +
                '}';
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
//        System.out.print(Instances.gson.toJson(finalQuery) + "\n");
//        SubQuery sexQuery = new SubQuery();
//        sexQuery.setFieldName("sex");
//        sexQuery.setMatchedValues(Lists.newArrayList("1"));
//
//        SubQuery finalQuery_ = new SubQuery();
//        finalQuery_.setLogic("AND");
//        finalQuery_.getSubQuerys().add(finalQuery);
//        finalQuery_.getSubQuerys().add(sexQuery);
//        System.out.print(Instances.gson.toJson(finalQuery_) + "\n");
//        String json = "{\"logic\":\"OR\",\"subQuerys\":[{\"logic\":\"AND\",\"fieldName\":\"age\",\"matchedValues\":[\"1988\"]},{\"logic\":\"AND\",\"fieldName\":\"height\",\"matchedValues\":[\"180\"]}]}";
//        SubQuery subQuery_final = Instances.gson.fromJson(json, SubQuery.class);
//        System.out.print(subQuery_final.toString());
    }

}
