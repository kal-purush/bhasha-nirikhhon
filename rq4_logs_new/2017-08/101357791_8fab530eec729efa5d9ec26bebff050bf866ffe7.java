package daris.client;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import arc.mf.client.AuthenticationDetails;


public class ConnectionSettings {

    public static final String PROPERTY_SERVER_HOST = "mf.host";
    public static final String PROPERTY_SERVER_PORT = "mf.port";
    public static final String PROPERTY_SERVER_TRANSPORT = "mf.transport";

    public static final String PROPERTY_APP = "mf.app";
    public static final String PROPERTY_TOKEN = "mf.token";
    public static final String PROPERTY_AUTH = "mf.auth";
    public static final String PROPERTY_DOMAIN = "mf.domain";
    public static final String PROPERTY_USER = "mf.user";
    public static final String PROPERTY_PASSWORD = "mf.password";
    public static final String PROPERTY_SID = "mf.sid";

    private String _app;
    private String _domain;
    private String _host;
    private Boolean _http = null;
    private String _password;
    private Integer _port = null;
    private Boolean _ssl = null;
    private String _token;
    private String _user;
    private String _sid;

    public ConnectionSettings(Properties properties) {
        loadFromProperties(properties);
    }

    public AuthenticationDetails authenticationDetails() {
        if (_token == null && (_domain == null || _user == null || _password == null)) {
            return null;
        } else {
            if (_token == null) {
                return new AuthenticationDetails(_app, _domain, _user, _password);
            } else {
                return new AuthenticationDetails(_app, _token);
            }
        }
    }

    public String domain() {
        return _domain;
    }

    public Boolean encrypt() {
        return _ssl;
    }

    public boolean hasAuthenticationDetails() {
        return hasToken() || hasUserCredentials();
    }

    public boolean hasSessionKey() {
        return _sid != null;
    }

    public boolean hasToken() {
        return _token != null;
    }

    public boolean hasUserCredentials() {
        return _domain != null && _user != null && _password != null;
    }

    protected void loadFromProperties(Properties properties) {
        if (properties != null) {
            if (properties.containsKey(PROPERTY_SERVER_HOST)) {
                _host = properties.getProperty(PROPERTY_SERVER_HOST);
            }
            if (properties.containsKey(PROPERTY_SERVER_PORT)) {
                String port = properties.getProperty(PROPERTY_SERVER_PORT);
                _port = Integer.parseInt(port);
            }
            if (properties.containsKey(PROPERTY_SERVER_TRANSPORT)) {
                String transport = properties.getProperty(PROPERTY_SERVER_TRANSPORT);
                setServerTransport(transport);
            }
            if (properties.containsKey(PROPERTY_APP)) {
                _app = properties.getProperty(PROPERTY_APP);
            }
            if (properties.containsKey(PROPERTY_TOKEN)) {
                _token = properties.getProperty(PROPERTY_TOKEN);
            }
            if (properties.containsKey(PROPERTY_AUTH)) {
                String auth = properties.getProperty(PROPERTY_AUTH);
                String[] parts = auth.split(",");
                if (parts == null || parts.length != 3) {
                    throw new IllegalArgumentException("Invalid property. " + PROPERTY_AUTH + ": " + auth);
                }
                _domain = parts[0];
                _user = parts[1];
                _password = parts[2];
            }
            if (properties.containsKey(PROPERTY_DOMAIN)) {
                _domain = properties.getProperty(PROPERTY_DOMAIN);
            }
            if (properties.containsKey(PROPERTY_USER)) {
                _user = properties.getProperty(PROPERTY_USER);
            }
            if (properties.containsKey(PROPERTY_PASSWORD)) {
                _password = properties.getProperty(PROPERTY_PASSWORD);
            }
            if (properties.containsKey(PROPERTY_SID)) {
                _sid = properties.getProperty(PROPERTY_SID);
            }
        }
    }

    protected void loadFromPropertiesFile(File propertiesFile) {
        Properties properties = new Properties();
        try {
            if (propertiesFile.exists()) {
                InputStream in = new BufferedInputStream(new FileInputStream(propertiesFile));
                try {
                    properties.load(in);
                } finally {
                    in.close();
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
        loadFromProperties(properties);
    }

    public String password() {
        return _password;
    }

    public String serverHost() {
        return _host;
    }

    public Integer serverPort() {
        return _port;
    }

    public String serverTransport() {
        if (_http) {
            if (_ssl) {
                return "https";
            } else {
                return "http";
            }
        } else {
            return "tcp/ip";
        }
    }

    public String sessionKey() {
        return _sid;
    }

    public ConnectionSettings setApp(String app) {
        _app = app;
        return this;
    }

    public ConnectionSettings setDomain(String domain) {
        _domain = domain;
        return this;
    }

    public ConnectionSettings setPassword(String password) {
        _password = password;
        return this;
    }

    public ConnectionSettings setServer(String host, int port, boolean useHttp, boolean encrypt) {
        _host = host;
        _port = port;
        _http = useHttp;
        _ssl = encrypt;
        return this;
    }

    public ConnectionSettings setServer(String host, int port, String transport) {
        _host = host;
        _port = port;
        setServerTransport(transport);
        return this;
    }

    public ConnectionSettings setServerHost(String host) {
        _host = host;
        return this;
    }

    public void setServerPort(int port) {
        _port = port;
    }

    public ConnectionSettings setServerTransport(String transport) {
        if ("http".equalsIgnoreCase(transport)) {
            _http = true;
            _ssl = false;
        } else if ("https".equalsIgnoreCase(transport)) {
            _http = true;
            _ssl = true;
        } else if (transport != null && (transport.startsWith("tcp") || transport.startsWith("TCP"))) {
            _http = false;
            _ssl = false;
        } else {
            throw new IllegalArgumentException("Invalid transport: " + transport);
        }
        return this;
    }

    public ConnectionSettings setSessionKey(String sessionKey) {
        _sid = sessionKey;
        return this;
    }

    public ConnectionSettings setToken(String token) {
        _token = token;
        return this;
    }

    public ConnectionSettings setUser(String user) {
        _user = user;
        return this;
    }

    public ConnectionSettings setUserCredentials(String domain, String user, String password) {
        _domain = domain;
        _user = user;
        _password = password;
        return this;
    }

    public String token() {
        return _token;
    }

    public String transport() {
        if (_http == null || _ssl == null) {
            return null;
        }
        return _http ? (_ssl ? "https" : "http") : "tcp/ip";
    }

    public Boolean useHttp() {
        return _http;
    }

    public String user() {
        return _user;
    }

    public void validate() throws Throwable {
        if (_host == null) {
            throw new IllegalArgumentException("Missing " + PROPERTY_SERVER_HOST);
        }
        if (_port == null) {
            throw new IllegalArgumentException("Missing " + PROPERTY_SERVER_PORT);
        }
        if (transport() == null) {
            throw new IllegalArgumentException("Missing " + PROPERTY_SERVER_TRANSPORT);
        }
        if (_token == null && (_domain == null || _user == null || _password == null) && _sid == null) {
            throw new IllegalArgumentException("Missing/Incomplete " + PROPERTY_TOKEN + " or " + PROPERTY_AUTH);
        }
    }

}