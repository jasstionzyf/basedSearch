/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.comm.basedSearch.service;


import com.comm.basedSearch.entity.AbstractQuery;
import com.comm.basedSearch.utils.SpringUtils;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * use to do some common task such as : save query log, process query result
 * cache... this implemention is thread safe
 *
 * @author jasstion
 */
public abstract class AbstractQueryService<Q extends AbstractQuery> {

    public final static ThreadLocal<Boolean> ifUseCache = new ThreadLocal<Boolean>();

    final static GsonBuilder g = new GsonBuilder();

    final static Gson gson = g.create();
    public final static String PRE_KEY ="querycachekey_";



    protected static final RedisTemplate<String, String> redisTemplate = (RedisTemplate<String, String>) SpringUtils.getBeanFromBeanContainer("redisTemplate");
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
        String cacheStr = baiheQuery.getCacheStrategy();
        String key = PRE_KEY + baiheQuery.hashCode();
        ValueOperations<String, String> valueOperations = redisTemplate.opsForValue();
        if (queryResult == null || queryResult.size() < 1) {
            return;
        }
        ifUseCache.set(Boolean.TRUE);
        String queryResultJson = gson.toJson(queryResult);
        if (cacheStr == null || cacheStr.trim().length() < 1) {
            //default not to set cache
            // valueOperations.set(key, queryResultJson);
        } else {
            valueOperations.set(key, queryResultJson, Integer.parseInt(cacheStr), TimeUnit.SECONDS);

        }
    }

    public List<Map<String, Object>> queryCache(Q baiheQuery) {
        List<Map<String, Object>> queryResult = null;
        String key = PRE_KEY + baiheQuery.hashCode();
        ValueOperations<String, String> valueOperations = redisTemplate.opsForValue();
        String queryResultJson = valueOperations.get(key);
        if (queryResultJson == null) {
            return null;
        }
        queryResult = gson.fromJson(queryResultJson, new TypeToken<ArrayList<Map<String, Object>>>() {
        }.getType());
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
