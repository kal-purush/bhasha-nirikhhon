package com.ulbra.config;

public class Configuracao {

    String serverName;
    String serverIP;
    int portListen;
    String memcachedServer;
    int memcachedPort;
    int[] yearData;

    public String getServerName() {
        return serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    public String getServerIP() {
        return serverIP;
    }

    public void setServerIP(String serverIP) {
        this.serverIP = serverIP;
    }

    public int getPortListen() {
        return portListen;
    }

    public void setPortListen(int portListen) {
        this.portListen = portListen;
    }

    public String getMemcachedServer() {
        return memcachedServer;
    }

    public void setMemcachedServer(String memcachedServer) {
        this.memcachedServer = memcachedServer;
    }

    public int getMemcachedPort() {
        return memcachedPort;
    }

    public void setMemcachedPort(int memcachedPort) {
        this.memcachedPort = memcachedPort;
    }

    public int[] getYearData() {
        return yearData;
    }

    public void setYearData(int[] yearData) {
        this.yearData = yearData;
    }

}