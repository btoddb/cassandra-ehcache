package com.btoddb.cache.rest;

import com.btoddb.cache.CachingService;
import com.btoddb.cache.CachedColumn;
import com.btoddb.cache.cassandra.CassandraDao;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.Map;


@Path("/v1")
public class RestServiceImpl {
    private CachingService cachingService;
    private CassandraDao cassandraDao;

    /**
     *
     * @param id
     * @param response
     * @return
     * @throws Exception
     */
    @GET
    @Path("/get/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, CachedColumn> get(
            @PathParam("id") final String id,
            @Context final HttpServletResponse response) throws Exception {
        Map<String, CachedColumn> row = cachingService.get(id);
        if (null == row) {
            row = cassandraDao.get(id);
            if (null != row) {
                cassandraDao.put(id, row);
            }
            else {
                response.sendError(HttpServletResponse.SC_NOT_FOUND, "Could not find object with ID = " + id);
            }
        }

        return row;
    }

    /**
     *
     * @param row
     */
    @PUT
    @Path("/put/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void put(
            @PathParam("id") String id,
            Map<String, CachedColumn> row) {
        cachingService.put(id, row);
        cassandraDao.put(id, row);
    }

    // ---------------------

    public CachingService getCachingService() {
        return cachingService;
    }

    public void setCachingService(CachingService cachingService) {
        this.cachingService = cachingService;
    }

    public CassandraDao getCassandraDao() {
        return cassandraDao;
    }

    public void setCassandraDao(CassandraDao cassandraDao) {
        this.cassandraDao = cassandraDao;
    }
}
