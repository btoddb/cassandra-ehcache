package com.btoddb.cache.cassandra;

import com.btoddb.cache.CachedColumn;
import com.btoddb.cache.EHCacheUpdateTrigger;
import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class CassandraDao {
    private static Logger logger = LoggerFactory.getLogger(CassandraDao.class);

    public static final int CQL_DEFAULT_PORT = 9042;
    public static final String DONT_INVALIDATE_CACHE_COLUMN = "###DONT_INVALIDATE_CACHE###";

    public static PreparedStatement getStatement;
    public static PreparedStatement putStatement;
    public static PreparedStatement deleteStatement;

    private String ksName = "cache";
    private int replicationFactor = 1;

    private Cluster cluster;
    private Session session;


    public void start(String node) {
        start(node, CQL_DEFAULT_PORT);
    }

    public void start(String node, int port) {
// driver 2.0 only
//        QueryOptions qo = new QueryOptions().setFetchSize(1).setConsistencyLevel(ConsistencyLevel.ONE);
//        .withQueryOptions(qo)
        cluster = Cluster.builder()
                .addContactPoint(node)
                .withPort(port)
                .build();
        Metadata metadata = cluster.getMetadata();
        if (logger.isInfoEnabled()) {
            logger.info("Connected to cluster: {}", metadata.getClusterName());
            for (Host host : metadata.getAllHosts()) {
                logger.info(String.format("Datacenter: %s; Host: %s; Rack: %s",
                            host.getDatacenter(), host.getAddress(), host.getRack()));
            }
        }
        session = cluster.connect();

        initializeSchema();
    }

    public void shutdown() {
        session.close();
    }

    public void initializeSchema() {
        // TODO:BTB - only create if doesn't exist or override
        createSchema();

        putStatement = session.prepare(
                String.format(
                        "INSERT INTO %s.data" +
                                "(id, name, value) " +
                                "VALUES (?, ?, ?);",
                        ksName));

        getStatement = session.prepare(
                String.format(
                        "SELECT name, value, writetime(value) AS ts FROM %s.data WHERE id = ?",
                        ksName));

        deleteStatement = session.prepare(
                String.format(
                        "DELETE value FROM %s.data WHERE id = ? and name = '%s'",
                        ksName, DONT_INVALIDATE_CACHE_COLUMN));

    }

    private void createSchema() {
        session.execute(String.format("DROP KEYSPACE IF EXISTS %s", ksName));
        session.execute(String.format("CREATE KEYSPACE %s WITH replication " +
                                        "= {'class':'SimpleStrategy', 'replication_factor':%d};", ksName, replicationFactor));

        session.execute(String.format(
                "CREATE TABLE %s.data (" +
                        "id varchar," +
                        "name varchar," +
                        "value varchar," +
                        "PRIMARY KEY ((id), name)" +
                        ")" +
                        "WITH COMPACT STORAGE;",
                        ksName));

        session.execute( String.format(
                "CREATE TRIGGER ehcacheUpdate ON %s.data USING '%s'",
                ksName, EHCacheUpdateTrigger.class.getName()));

    }

    /**
     *
     * @param id
     * @return
     */
    public Map<String, CachedColumn> get(String id) {
        ResultSet res = session.execute(getStatement.bind(id));
        Map<String, CachedColumn> data = new HashMap<>();
        for (Row row : res) {
            String name = row.getString("name");
            if (DONT_INVALIDATE_CACHE_COLUMN.equals(name)) {
                continue;
            }
            String value = row.getString("value");
            long ts = row.getLong("ts");

            data.put(name, new CachedColumn(name, value, ts));
        }
        return !data.isEmpty() ? data : null;
    }

    /**
     * insert data into row with key = id.
     *
     * @param id row key
     * @param data columns
     */
    public void put(String id, Map<String, CachedColumn> data) {
        BatchStatement batch = new BatchStatement();
        batch.add(deleteStatement.bind(id));
        for (CachedColumn col : data.values()) {
            batch.add(putStatement.bind(id, col.getName(), col.getData().toString()));
        }
        session.execute(batch);
    }
}
