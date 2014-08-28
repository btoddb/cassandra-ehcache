
package com.btoddb.cache.cassandra;

import org.apache.cassandra.cli.CliMain;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;

import java.io.*;


/**
 * This is a helper class (or to be derived by a JUnit) that will startup an
 * embedded Cassandra server, listening on localhost:9170 by default.
 *
 * <p/>There server will only be started once, no matter how many times #startEmbeddedServer
 * is called.  And it *cannot* be stopped until the JVM is stopped.  However your JUnit
 * can call #createKeyspaceFromCliFile as many times as you like.  And it is a good
 * practice to create/drop/recreate with every test run.
 *
 * <p/>This class can also be used to setup a remote cluster for testing simply
 * by calling #createKeyspaceFromCliFile after setting host and port properly.
 */
public class CassandraTestHelper {
    protected static CassandraEmbeddedServer embedded;
    static public String host = "localhost";
    static public int port = 9170;

    public static void startEmbeddedServer(String yamlFile) throws TTransportException, IOException,
            InterruptedException, ConfigurationException {
        // we only start cassandra *once* for the entire JVM (test suite)
        if ( null != embedded ) {
            return;
        }

        embedded = new CassandraEmbeddedServer(yamlFile);
        embedded.setup();

        // make sure startup finished and can cannect
        for (int i = 0; i < 10; i++) {
            try {
                CliMain.connect(host, port);
                CliMain.disconnect();
                break;
            } catch (Throwable e) {
                // wait, then retry
                Thread.sleep(500);
            }
        }
    }

    public static void createKeyspaceFromCliFile(String fileName) throws Exception {
        // new error/output streams for CliSessionState
        ByteArrayOutputStream errStream = new ByteArrayOutputStream();
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();

        // checking if we can connect to the running cassandra node on localhost
        CliMain.connect(host, port);

        try {
            // setting new output stream
            CliMain.sessionState.setOut(new PrintStream(outStream));
            CliMain.sessionState.setErr(new PrintStream(errStream));

            // read schema from file
            BufferedReader fr;
            try {
                fr = new BufferedReader(new FileReader(fileName));
            }
            catch ( FileNotFoundException e ) {
                fr = new BufferedReader( new FileReader(CassandraTestHelper.class.getResource("/"+fileName).getFile()) );
            }

            String line;
            StringBuffer sb = new StringBuffer();
            while (null != (line = fr.readLine())) {
                line = line.trim();
                if (line.startsWith("#")) {
                    continue;
                } else if (line.startsWith("quit")) {
                    break;
                }

                sb.append(line + " ");
                if (line.endsWith(";")) {
                    try {
                        CliMain.processStatement(sb.toString());
                        // String result = outStream.toString();
                        outStream.toString();

                        outStream.reset(); // reset stream so we have only
                        // output
                        // from
                        // next statement all the time
                        errStream.reset(); // no errors to the end user.
                    } catch (Throwable e) {
                        // ignore
                    }

                    sb = new StringBuffer();
                }
            }
        } finally {
            if (CliMain.isConnected()) {
                CliMain.disconnect();
            }
        }
    }

    public static void shutdownEmbeddedServer() {
        CassandraEmbeddedServer.shutdown();
    }
}
