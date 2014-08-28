package com.btoddb.cache;

import com.btoddb.cache.cassandra.CassandraTestHelper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cxf.helpers.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;


public class CassandraExternalCacheIT {
    ObjectMapper objMapper = new ObjectMapper();
    CassandraExternalCache cache;
    long now;

    String idInCache = "123";
    CachedColumn colInCache = new CachedColumn("cached", "yes", now-1000);

    String idNotInCache = "456";
    CachedColumn colNotInCache = new CachedColumn("cached", "no", now-1000);

    @BeforeClass
    public static void setupCass() throws Exception {
        CassandraTestHelper.startEmbeddedServer("/cassandra.yaml");
    }

    @Before
    public void setup() {
        cache = new CassandraExternalCache();
        now = System.currentTimeMillis();

        cache.getCachingService().resetCache();
        cache.getCachingService().put(idInCache, Collections.singletonMap(colInCache.getName(), colInCache));
        cache.getCassandraDao().put(idInCache, Collections.singletonMap(colInCache.getName(), colInCache));

        cache.getCassandraDao().put(idNotInCache, Collections.singletonMap(colNotInCache.getName(), colNotInCache));
    }

    @After
    public void teardown() {
        cache.shutdown();
    }

    @Test
    public void testReturnObjectFromCache() throws Exception {
        URL url = new URL("http://localhost:9090/v1/get/"+idInCache);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        try {
            assertThat(conn.getResponseMessage(), conn.getResponseCode(), is(200));
            assertThat(conn.getContentType(), is("application/json"));

            String json = IOUtils.toString(conn.getInputStream());
            System.out.println("data = " + json);

            Map<String, CachedColumn> map = objMapper.readValue(json, new TypeReference<Map<String, CachedColumn>>() {});
            assertThat(map.entrySet(), hasSize(1));
            assertThat(map, hasEntry(colInCache.getName(), colInCache));

            assertThat(cache.getCachingService().hits(), is(1L));
            assertThat(cache.getCachingService().misses(), is(0L));
            assertThat(cache.getCachingService().puts(), is(1L));
            assertThat(cache.getCassandraDao().get(idInCache), is(notNullValue()));
        }
        finally {
            conn.getInputStream().close();
        }
    }

    @Test
    public void testReturnObjectFromCassandra() throws Exception {
        URL url = new URL("http://localhost:9090/v1/get/"+idNotInCache);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        try {
            assertThat(conn.getResponseMessage(), conn.getResponseCode(), is(200));
            assertThat(conn.getContentType(), is("application/json"));

            String json = IOUtils.toString(conn.getInputStream());
            System.out.println("data = " + json);

            Map<String, CachedColumn> map = objMapper.readValue(json, new TypeReference<Map<String, CachedColumn>>() {});
            assertThat(map.entrySet(), hasSize(1));
            assertThat(map, hasKey(colNotInCache.getName()));

            assertThat(cache.getCachingService().hits(), is(0L));
            assertThat(cache.getCachingService().misses(), is(1L));
            assertThat(cache.getCachingService().puts(), is(1L));
            assertThat(cache.getCassandraDao().get(idInCache), is(notNullValue()));
        }
        finally {
            conn.getInputStream().close();
        }
    }

    @Test
    public void testUpdateViaTrigger() throws Exception {
        CachedColumn updatedColumn = new CachedColumn("cached", "yes", now-1000);

        cache.getCassandraDao().put(idInCache, Collections.singletonMap(updatedColumn.getName(), updatedColumn));

        assertThat(cache.getCachingService().hits(), is(0L));
        assertThat(cache.getCachingService().misses(), is(0L));
        assertThat(cache.getCachingService().puts(), is(1L));
    }

    @Test
    public void testInvalidId() throws Exception {
        URL url = new URL("http://localhost:9090/v1/get/does-not-exist");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        assertThat(conn.getResponseCode(), is(404));
    }
}
