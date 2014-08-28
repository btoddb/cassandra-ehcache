package com.btoddb.cache.cassandra;

import com.btoddb.cache.CachedColumn;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;


public class CassandraDaoTest {
    long now = System.currentTimeMillis();
    CassandraDao dao;

    @BeforeClass
    public static void setupCass() throws Exception {
        CassandraTestHelper.startEmbeddedServer("/cassandra.yaml");
    }

    @Before
    public void setup() {
        dao = new CassandraDao();
        dao.start("localhost", 9052);
    }

    @After
    public void teardown() {
        dao.shutdown();
    }

    @Test
    public void testPutThenGet() throws Exception {
        Map<String, CachedColumn> row = new HashMap<>();
        row.put("first", new CachedColumn("first", "first-data", now));
        row.put("second", new CachedColumn("second", "second-data", now));

        dao.put("1", row);

        Map<String, CachedColumn> resp = dao.get("1");

        assertThat(resp.entrySet(), hasSize(row.size()));
        for (CachedColumn col : row.values()) {
            assertThat(resp, hasKey(col.getName()));
        }
    }

    @Test
    public void testRowNotFound() {
        Map<String, CachedColumn> resp = dao.get("not-found-key");
        assertThat(resp, is(nullValue()));
    }
}