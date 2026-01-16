package br.lassal.dbvcs.tatubola.maven;

public class DBEnvironment {

    private String name;
    private String jdbcUrl;
    private PlainCredentials credentials;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public PlainCredentials getCredentials() {
        return credentials;
    }

    public void setCredentials(PlainCredentials credentials) {
        this.credentials = credentials;
    }
}