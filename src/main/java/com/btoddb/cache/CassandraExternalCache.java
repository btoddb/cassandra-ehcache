package com.btoddb.cache;

import com.btoddb.cache.cassandra.CassandraDao;
import com.btoddb.cache.rest.RestServiceImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.base.JsonMappingExceptionMapper;
import com.fasterxml.jackson.jaxrs.base.JsonParseExceptionMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.apache.cxf.jaxrs.JAXRSServerFactoryBean;
import org.apache.cxf.endpoint.Server;

import java.util.Arrays;



public class CassandraExternalCache {
    public static final String KEYSPACE = "cache";
    public static final String COLUMN_FAMILY = "data";

    // must do this so the c* trigger can find the shared caching service
    public static CachingService cachingService = new CachingServiceUsingEhcache(
            CassandraExternalCache.KEYSPACE, CassandraExternalCache.COLUMN_FAMILY);

    JAXRSServerFactoryBean serverFactoryBean;
    Server jaxrsServer;
    RestServiceImpl restService;
    CassandraDao cassandraDao;
    Config config = new Config();


    public CassandraExternalCache() {
        init();
    }

    protected void init() {
        config.readConfig();

        initializeCassandraDao();
//        initializeCachingService();
        initializeRestService();
    }

    protected void initializeCassandraDao() {
        cassandraDao = new CassandraDao();
        cassandraDao.start(config.getCassandraCqlHost(), config.getCassandraCqlPort());
    }

//    protected void initializeCachingService() {
//    }

    protected void initializeRestService() {
        restService = new RestServiceImpl();
        restService.setCachingService(cachingService);
        restService.setCassandraDao(cassandraDao);

        ObjectMapper objMap = new ObjectMapper();
        JacksonJaxbJsonProvider jsonProvider = new JacksonJaxbJsonProvider();
        jsonProvider.setMapper(objMap);

        serverFactoryBean = new JAXRSServerFactoryBean();
        serverFactoryBean.setAddress("http://0.0.0.0:9090");
        serverFactoryBean.setProviders(Arrays.asList(
                                               jsonProvider,
                                               new JsonMappingExceptionMapper(),
                                               new JsonParseExceptionMapper())
        );
        serverFactoryBean.setServiceBean(restService);

        jaxrsServer = serverFactoryBean.create();
        jaxrsServer.start();
    }

    public void shutdown() {
        jaxrsServer.stop();
    }

    public CachingService getCachingService() {
        return cachingService;
    }

    public void setCachingService(CachingService cachingService) {
        CassandraExternalCache.cachingService = cachingService;
    }

    public CassandraDao getCassandraDao() {
        return cassandraDao;
    }

    public void setCassandraDao(CassandraDao cassandraDao) {
        this.cassandraDao = cassandraDao;
    }
}
