/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.comm.basedSearch.service;


import com.comm.basedSearch.entity.AbstractQuery;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * use to do some common task such as : save query log, process query result
 * cache... this implemention is thread safe
 *
 * @author jasstion
 */
public abstract class AbstractQueryService<Q extends AbstractQuery> {


    final static GsonBuilder g = new GsonBuilder();

    final static Gson gson = g.create();



    protected final static Logger LOGGER = LoggerFactory.getLogger(AbstractQueryService.class);
    public static Set<String> filterFieldNmae = Sets.newHashSet("shape", "residencepProvince", "province", "education", "corporationNature", "industry", "income", "loveType", "matchIncome", "lastLoginDate");

    public List<Map<String, Object>> processQuery(Q baiheQuery) throws Exception {

        long timeTaken = 0;
        boolean cacheHit = false;
        List<Map<String, Object>> queryResult = null;
        StopWatch stopWatch = new StopWatch();

        // Start the watch, do some task and stop the watch.
        stopWatch.start();

        queryResult = this.queryCache(baiheQuery);
        if (queryResult != null && queryResult.size() > 1) {
            cacheHit = true;
            AbstractQueryService.QueryStatistics queryStatistics = new QueryStatistics(timeTaken, cacheHit);
            stopWatch.stop();
            timeTaken = stopWatch.getTime();
            this.recordQueryLog(queryStatistics);
            return queryResult;
        }
        cacheHit = false;
        queryResult = this.query(baiheQuery);
        stopWatch.stop();
        timeTaken = stopWatch.getTime();

        AbstractQueryService.QueryStatistics queryStatistics = new QueryStatistics(timeTaken, cacheHit);
        this.recordQueryLog(queryStatistics);
        this.cacheQueryResult(baiheQuery, queryResult);

        return queryResult;

    }

    public abstract List<Map<String, Object>> query(Q baiheQuery) throws Exception;

    public void cacheQueryResult(Q baiheQuery, List<Map<String, Object>> queryResult) {

    }

    public List<Map<String, Object>> queryCache(Q baiheQuery) {
        List<Map<String, Object>> queryResult = null;

        return queryResult;

    }

    public void recordQueryLog(QueryStatistics queryStatistics) {
        LOGGER.info(queryStatistics.toString());

    }

    public static class QueryStatistics {

        //seconds
        private long queryTakenTime = 0;
        private boolean cacheHit = false;

        @Override
        public String toString() {
            return "QueryStatistics{" + "queryTakenTime=" + queryTakenTime + ", cacheHit=" + cacheHit + '}';
        }

        public QueryStatistics(long queryTakenTim, boolean cacheHit) {
            this.queryTakenTime = queryTakenTim;
            this.cacheHit = cacheHit;
        }

    }

}
