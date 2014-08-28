package com.btoddb.cache;

import java.util.Map;


/**
 *
 */
public interface CachingService {

    Map<String, CachedColumn> get(String key);

    void put(String key, Map<String, CachedColumn> data);

    boolean contains(String key);

    void acquireWriteLockOnKey(String key);

    void releaseWriteLockOnKey(String key);

    long hits();

    long misses();

    long puts();

    void resetCache();
}
