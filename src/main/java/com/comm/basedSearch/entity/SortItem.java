/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.comm.basedSearch.entity;

import java.io.Serializable;

/**
 *
 * @author jasstion
 */
public class SortItem implements Serializable{

    public SortItem() {
        super();
    }
    
    protected String fieldName;
    //age:asc or age:desc
    protected String sort;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getSort() {
        return sort;
    }

    public void setSort(String sort) {
        this.sort = sort;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 53 * hash + (this.fieldName != null ? this.fieldName.hashCode() : 0);
        hash = 53 * hash + (this.sort != null ? this.sort.hashCode() : 0);
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
        final SortItem other = (SortItem) obj;
        if ((this.fieldName == null) ? (other.fieldName != null) : !this.fieldName.equals(other.fieldName)) {
            return false;
        }
        return true;
    }
    
    
}
