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
public class AdvancedCommQuery extends AbstractQuery {

    private SubQuery subQuery = new SubQuery();

    public AdvancedCommQuery(int pageNum, int pageSize) {
        super(pageNum, pageSize);
    }

    public AdvancedCommQuery() {
        super();
    }

    public AdvancedCommQuery(int pageNum, int pageSize, List<SortItem> sortItems, List<String> fls) {
        super(pageNum, pageSize, sortItems, fls);
    }

    @Override
    public String toString() {
        return "AdvancedCommQuery{" + "subQuery=" + subQuery + '}';
    }

    public SubQuery getSubQuery() {
        return subQuery;
    }

    public void setSubQuery(SubQuery subQuery) {
        this.subQuery = subQuery;
    }

}
