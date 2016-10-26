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
public class QueryItem implements Serializable {

    boolean isFilterType = false;

    public boolean isPayload() {
        return isPayload;
    }

    public void setIsPayload(boolean isPayload) {
        this.isPayload = isPayload;
    }

    boolean isPayload=false;
    String fieldName = null;
    // range: * TO 100, 2015-07-06T08:52:48Z TO 2015-07-06T08:52:48Z,  disperse: 20, "married"...    -means not match following query
    List<String> matchedValues = Lists.newArrayList();

    public QueryItem(String fieldName, List<String> matchedValues) {
        this.fieldName = fieldName;
        this.matchedValues = matchedValues;
    }

    public QueryItem() {
        super();
    }

    public boolean isIsFilterType() {
        return isFilterType;
    }

    public void setIsFilterType(boolean isFilterType) {
        this.isFilterType = isFilterType;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public List<String> getMatchedValues() {
        return matchedValues;
    }

    public void setMatchedValues(List<String> matchedValues) {
        this.matchedValues = matchedValues;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 43 * hash + (this.fieldName != null ? this.fieldName.hashCode() : 0);
        hash = 43 * hash + (this.matchedValues != null ? this.matchedValues.hashCode() : 0);
        return hash;
    }

   
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final QueryItem other = (QueryItem) obj;
        if ((this.fieldName == null) ? (other.fieldName != null) : !this.fieldName.equals(other.fieldName)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "QueryItem{" + "isFilterType=" + isFilterType + ", fieldName=" + fieldName + ", matchedValues=" + matchedValues + '}';
    }
    
}
