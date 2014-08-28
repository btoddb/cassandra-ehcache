package com.btoddb.cache;

import com.btoddb.cache.cassandra.CassandraDao;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;


/**
 * Use the C* Trigger mechanism to update/invalidate cache entries in EHCache.
 *
 */
public class EHCacheUpdateTrigger implements ITrigger {
    private static Logger logger = LoggerFactory.getLogger(EHCacheUpdateTrigger.class);

    public static final ByteBuffer DONT_INVALIDATE_COLUMN = ByteBuffer.wrap(CassandraDao.DONT_INVALIDATE_CACHE_COLUMN.getBytes());


    public EHCacheUpdateTrigger(){
    }

    @Override
    public Collection<RowMutation> augment(ByteBuffer key, ColumnFamily update) {
        try {
            updateCache(key, update);
        }
        catch (Throwable e) {
            logger.error("exception while updating Ehcache", e);
        }

        return null;
    }

    public void updateCache(ByteBuffer key, ColumnFamily update) throws Throwable {
        // cache key is the row key
        // we make a dupe of the ByteBuffer so we can manipulate the position
        String cacheKeyAsStr = ByteBufferUtil.string(key);

        // if exists, remove the *don't invalidate the cache* column, then return
        for (Column col : update) {
            if (col instanceof DeletedColumn && DONT_INVALIDATE_COLUMN.equals(col.name())) {
                return;
            }
        }

        // if key is not in cache, then do nothing - we only update the cache, not load
        // external process is expected to do "read-through" type operations
        if (!CassandraExternalCache.cachingService.contains(cacheKeyAsStr)) {
            return;
        }

        // get write lock on cache key so we can update the structure
        CassandraExternalCache.cachingService.acquireWriteLockOnKey(cacheKeyAsStr);
        try {
            Map<String, CachedColumn> cachedRow = CassandraExternalCache.cachingService.get(cacheKeyAsStr);

            // now that we have the lock, check one more time that the row still exists in cache
            // if doesn't we don't have anything to update
            if (null == cachedRow) {
                return;
            }

            // iterate over mutated columns, updating the cache structure
            for (Column col : update) {
                ColumnDefinition colDef = update.metadata().getColumnDefinitionFromColumnName(col.name());

                // the "primary key" column isn't a real cassandra column, it is the row key and no definition
                if (null == colDef) {
                    continue;
                }

                // cached column entry is referenced by C* column name
                // ByteBufferUtil handles calling ByteBuffer.duplicate
                String name = ByteBufferUtil.string(col.name());
                CachedColumn entry = cachedRow.get(name);

                // if cached column not found, then add it
                if (null == entry) {
                    cachedRow.put(name, new CachedColumn(ByteBufferUtil.string(col.name()), composeObjectUsingValidator(colDef, col.value()), col.timestamp()));
                }
                // only update this cached column if the mutation is "newer" than what is already in the cache.
                // make sure the timestamp units are well understood - typical default in C* is microseconds
                else if (col.timestamp() > entry.getTimestamp()) {
                    entry.setTimestamp(col.timestamp());
                    entry.setData(colDef.getValidator().compose(col.value()));
                }
            }
        }
        finally {
            CassandraExternalCache.cachingService.releaseWriteLockOnKey(cacheKeyAsStr);
        }
    }

    private Object composeObjectUsingValidator(ColumnDefinition colDef, ByteBuffer data) {
        // compose handles calling ByteBuffer.duplicate
        return colDef.getValidator().compose(data);
    }
}
