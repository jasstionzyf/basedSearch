/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.comm.basedSearch.service;


/**
 *
 * @author jasstion
 */
public interface QueryGenerator<T,Q>{
    public T generateFinalQuery(Q query);
    
}
