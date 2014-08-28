package com.btoddb.cache;


public class Config {
    private String cassandraCqlHost = "localhost";
    private int cassandraCqlPort = 9052;


    public String getCassandraCqlHost() {
        return cassandraCqlHost;
    }

    public void setCassandraCqlHost(String cassandraCqlHost) {
        this.cassandraCqlHost = cassandraCqlHost;
    }

    public int getCassandraCqlPort() {
        return cassandraCqlPort;
    }

    public void setCassandraCqlPort(int cassandraCqlPort) {
        this.cassandraCqlPort = cassandraCqlPort;
    }

    public void readConfig() {

        // TODO:BTB - uh, do it
    }
}
