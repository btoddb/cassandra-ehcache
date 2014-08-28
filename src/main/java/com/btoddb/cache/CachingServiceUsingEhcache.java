package com.btoddb.cache;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.PersistenceConfiguration;

import java.util.Map;


/**
 */
public class CachingServiceUsingEhcache implements CachingService {
    private static final CacheManager cacheManager = new CacheManager();
    static {
        CacheConfiguration conf = cacheManager.getConfiguration().getDefaultCacheConfiguration();
        conf.getPersistenceConfiguration().strategy(PersistenceConfiguration.Strategy.NONE);
    }

    private final String keyspaceName;
    private final String columnFamilyName;

    public CachingServiceUsingEhcache(String keyspaceName, String columnFamilyName) {
        this.keyspaceName = keyspaceName;
        this.columnFamilyName = columnFamilyName;

    }

    @Override
    public Map<String, CachedColumn> get(String key) {
        Element elem = getCache(keyspaceName, columnFamilyName).get(key);
        return null != elem ? (Map<String, CachedColumn>) elem.getObjectValue() : null;
    }

    @Override
    public void put(String key, Map<String, CachedColumn> data) {
        Cache theCache = getCache(keyspaceName, columnFamilyName);
        theCache.put(new Element(key, data));
    }

    @Override
    public boolean contains(String key) {
        return getCache(keyspaceName, columnFamilyName).isKeyInCache(key);
    }

    @Override
    public void acquireWriteLockOnKey(String key) {
        getCache(keyspaceName, columnFamilyName).acquireWriteLockOnKey(key);
    }

    @Override
    public void releaseWriteLockOnKey(String key) {
        getCache(keyspaceName, columnFamilyName).releaseWriteLockOnKey(key);
    }

    @Override
    public long hits() {
        return getCache(generateCacheName(keyspaceName, columnFamilyName)).getStatistics().cacheHitCount();
    }

    @Override
    public long misses() {
        return getCache(generateCacheName(keyspaceName, columnFamilyName)).getStatistics().cacheMissCount();
    }

    @Override
    public long puts() {
        return getCache(generateCacheName(keyspaceName, columnFamilyName)).getStatistics().cachePutCount();
    }

    @Override
    public void resetCache() {
        cacheManager.removeCache(generateCacheName(keyspaceName, columnFamilyName));
    }

    String generateCacheName(String keyspaceName, String columnFamilyName) {
        return keyspaceName+":"+columnFamilyName;
    }

    Cache getCache(String keyspaceName, String columnFamilyName) {
        return getCache(generateCacheName(keyspaceName, columnFamilyName));
    }

    private Cache getCache(String cacheName) {
        if (!cacheManager.cacheExists(cacheName)) {
            cacheManager.addCacheIfAbsent(cacheName);
        }
        return cacheManager.getCache(cacheName);
    }
}
