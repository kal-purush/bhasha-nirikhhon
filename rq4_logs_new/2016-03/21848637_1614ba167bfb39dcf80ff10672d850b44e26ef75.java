package helper;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPDigestAuthFilter;
import com.sun.jersey.client.urlconnection.HTTPSProperties;

import models.Globals;
import play.Play;

class HeritrixWebclient {
    public static Client createWebclient() {
	play.Logger.debug("Hello Heritrix");
	String keystoreLoc = Play.application().configuration()
		    .getString("regal-api.keystoreLocation");
	 String keystorePass = Play.application().configuration()
		    .getString("regal-api.keystorePassword");
	if (keystoreLoc == null || keystorePass == null || keystoreLoc.isEmpty() || keystorePass.isEmpty()) {
	    play.Logger.error(
		    "Keystore is not configured. Set regal-api.keystoreLocation and regal-api.keystorePassword in application.conf");
	    return null;
	}
	String heritrixUser = Play.application().configuration().getString("regal-api.heritrix.user");
	String heritrixPwd = Play.application().configuration().getString("regal-api.heritrix.pwd");

	Client webclient = null;
	ClientConfig cc = new DefaultClientConfig();
	// cc.getProperties()
	// .put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, true);
	cc.getFeatures().put(ClientConfig.FEATURE_DISABLE_XML_SECURITY, true);
	cc.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES,
		new HTTPSProperties(null, initSsl(cc, keystoreLoc, keystorePass)));
	webclient = Client.create(cc);
	webclient.addFilter(new HTTPDigestAuthFilter(heritrixUser, heritrixPwd));
	// 10min
	webclient.setConnectTimeout(1000 * 60 * 10);
	// 10sec
	webclient.setReadTimeout(1000 * 10);
	return webclient;
    }

    private static SSLContext initSsl(ClientConfig cc, String keystorelocation, String keystorepasswd) {
	try (InputStream keystoreInput = new FileInputStream(keystorelocation)) {
	    SSLContext ctx = SSLContext.getInstance("SSL");
	    KeyStore trustStore;
	    trustStore = KeyStore.getInstance("JKS");
	    trustStore.load(keystoreInput, keystorepasswd.toCharArray());
	    TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
	    tmf.init(trustStore);
	    ctx.init(null, tmf.getTrustManagers(), null);
	    return ctx;
	} catch (Exception e) {
	    throw new RuntimeException("Can not initiate SSL connection", e);
	}
    }
}