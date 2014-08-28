package com.btoddb.cache.cassandra;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;


/**
 *
 * @author Ran Tavory (rantav@gmail.com)
 *
 */
public class CassandraEmbeddedServer {
    private static Logger log = LoggerFactory.getLogger(CassandraEmbeddedServer.class);

    private static final String TMP = "tmp";

    private final String yamlFile;
    static CassandraDaemon cassandraDaemon;

    public CassandraEmbeddedServer() {
        this("/cassandra.yaml");
    }

    public CassandraEmbeddedServer(String yamlFile) {
        this.yamlFile = yamlFile;
    }

    static ExecutorService executor = Executors.newSingleThreadExecutor( new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("cassandra-executor");
            t.setDaemon(true);
            return t;
        }
    });

    /**
     * Set embedded cassandra up and spawn it in a new thread.
     *
     * @throws org.apache.thrift.transport.TTransportException
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    public void setup() throws TTransportException, IOException,
            InterruptedException, ConfigurationException {
        // delete tmp dir first
        rmdir(TMP);

        // make a tmp dir and copy cassandra.yaml and log4j.properties to it
        copy("/log4j.properties", TMP);
        copy(yamlFile, TMP);

        // yamlFile may be user defined; make sure to grab the copy directly from TMP
        System.setProperty("cassandra.config", "file:" + TMP + "/" + yamlFile.substring(yamlFile.lastIndexOf("/") + 1));
        System.setProperty("log4j.configuration", "file:" + TMP + "/log4j.properties");
        System.setProperty("cassandra-foreground","true");

        cleanupAndLeaveDirs();

        log.info("Starting executor");

        executor.execute(new CassandraRunner());
        log.info("Cassandra executor started ... but C* may not be ready for requests yet");
    }



    public static void shutdown() {
        executor.shutdown();
        executor.shutdownNow();
        log.info("shutdown complete");
    }

    private static void rmdir(String dir) throws IOException {
        File dirFile = new File(dir);
        if (dirFile.exists()) {
            FileUtils.deleteRecursive(new File(dir));
        }
    }

    /**
     * Copies a resource from within the jar to a directory.
     */
    private static void copy(String resource, String directory)
            throws IOException {
        FileUtils.createDirectory(directory);
        InputStream is = CassandraEmbeddedServer.class.getResourceAsStream(resource);
        String fileName = resource.substring(resource.lastIndexOf("/") + 1);
        File file = new File(directory + System.getProperty("file.separator")
                + fileName);
        OutputStream out = new FileOutputStream(file);
        byte buf[] = new byte[1024];
        int len;
        while ((len = is.read(buf)) > 0) {
            out.write(buf, 0, len);
        }
        out.close();
        is.close();
    }

    public static void cleanupAndLeaveDirs() throws IOException
    {
        mkdirs();
        cleanup();
        mkdirs();
        CommitLog.instance.resetUnsafe(); // cleanup screws w/ CommitLog, this brings it back to safe state
    }

    public static void cleanup() throws IOException
    {
        // clean up commitlog
        String[] directoryNames = { DatabaseDescriptor.getCommitLogLocation(), };
        for (String dirName : directoryNames)
        {
            File dir = new File(dirName);
            if (!dir.exists())
                throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
            FileUtils.deleteRecursive(dir);
        }

        // clean up data directory which are stored as data directory/table/data files
        for (String dirName : DatabaseDescriptor.getAllDataFileLocations())
        {
            File dir = new File(dirName);
            if (!dir.exists())
                throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
            FileUtils.deleteRecursive(dir);
        }
    }

    public static void mkdirs()
    {
        DatabaseDescriptor.createAllDirectories();
    }


    class CassandraRunner implements Runnable {

        @Override
        public void run() {
            cassandraDaemon = new CassandraDaemon();
            cassandraDaemon.activate();
        }

    }
}
