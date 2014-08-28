package com.btoddb.cache.rest;

import com.btoddb.cache.CachedColumn;
import com.btoddb.cache.CachingService;
import com.btoddb.cache.cassandra.CassandraDao;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletResponse;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;


public class RestServiceImplTest {
    CachingService cachingService;
    CassandraDao cassandraDao;
    RestServiceImpl restService;

    Map<String, CachedColumn> mock123 = mock(Map.class);
    Map<String, CachedColumn> mock456 = mock(Map.class);

    @Before
    public void setup() {
        cachingService = mock(CachingService.class);
        when(cachingService.get("123")).thenReturn(mock123);
        when(cachingService.get("456")).thenReturn(null);
        when(cachingService.get("789")).thenReturn(null);

        cassandraDao = mock(CassandraDao.class);
        when(cassandraDao.get("456")).thenReturn(mock456);
        when(cassandraDao.get("789")).thenReturn(null);

        restService = new RestServiceImpl();
        restService.setCachingService(cachingService);
        restService.setCassandraDao(cassandraDao);
    }

    @Test
    public void testGetFromCache() throws Exception {
        Map<String, CachedColumn> resp = restService.get("123", null);

        assertThat(resp, is(mock123));
        verify(cachingService, times(1)).get("123");
        verify(cachingService, times(0)).put(anyString(), anyMap());
        verify(cassandraDao, times(0)).get(anyString());
        verify(cassandraDao, times(0)).put(anyString(), anyMap());
        verifyNoMoreInteractions(cachingService, cassandraDao);
    }

    @Test
    public void testGetFromCassandra() throws Exception {
        HttpServletResponse httpResp = mock(HttpServletResponse.class);

        Map<String, CachedColumn> resp = restService.get("456", httpResp);

        assertThat(resp, is(mock456));
        verify(cachingService, times(1)).get("456");
        verify(cachingService, times(0)).put(anyString(), anyMap());
        verify(cassandraDao, times(1)).get("456");
        verify(cassandraDao, times(1)).put(eq("456"), eq(mock456));
        verifyNoMoreInteractions(cachingService, cassandraDao);
    }

    @Test
    public void testGetCompleteMiss() throws Exception {
        HttpServletResponse httpResp = mock(HttpServletResponse.class);

        Map resp = restService.get("789", httpResp);

        assertThat(resp, is(nullValue()));
        verify(httpResp).sendError(eq(HttpServletResponse.SC_NOT_FOUND), anyString());
        verify(cachingService, times(1)).get("789");
        verify(cachingService, times(0)).put(anyString(), anyMap());
        verify(cassandraDao, times(1)).get("789");
        verify(cassandraDao, times(0)).put(anyString(), anyMap());
        verifyNoMoreInteractions(cachingService, cassandraDao);
    }

    @Test
    public void testPutData() throws Exception {
        restService.put("123", mock123);

        verify(cachingService, times(0)).get(anyString());
        verify(cachingService, times(1)).put("123", mock123);
        verify(cassandraDao, times(0)).get(anyString());
        verify(cassandraDao, times(1)).put("123", mock123);
        verifyNoMoreInteractions(cachingService, cassandraDao);

    }
}