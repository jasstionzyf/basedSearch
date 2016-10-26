/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.comm.basedSearch.entity;

import java.util.List;

/**
 *
 * @author jasstion
 */
public class EsCommonQuery extends CommonQuery {
    
    
    private String index = null;
    private String type = null;






    /* elasticsearch script score string */
    private String scoreScript=null;

    public String getScoreScript() {
        return scoreScript;
    }

    public void setScoreScript(String scoreScript) {
        this.scoreScript = scoreScript;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public EsCommonQuery() {
        super();
    }

    public EsCommonQuery(List queryItems, int pageNum, int pageSize, List<SortItem> sortItems, List<String> fls, String index, String type) {
        super(queryItems, pageNum, pageSize, sortItems, fls);
        this.index = index;
        this.type = type;

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EsCommonQuery that = (EsCommonQuery) o;

        if (index != null ? !index.equals(that.index) : that.index != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        return !(scoreScript != null ? !scoreScript.equals(that.scoreScript) : that.scoreScript != null);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (index != null ? index.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (scoreScript != null ? scoreScript.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "EsCommonQuery{" +
                "index='" + index + '\'' +
                ", type='" + type + '\'' +
                ", scoreScript='" + scoreScript + '\'' +
                "} " + super.toString();
    }
}
