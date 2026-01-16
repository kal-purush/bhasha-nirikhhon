package es.caib.seycon.idp.server;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLEncoder;

import javax.servlet.http.HttpServletRequest;

import org.json.JSONObject;
import org.json.JSONTokener;

import com.soffid.iam.addons.federation.common.FederationMember;

import es.caib.seycon.ng.exception.InternalErrorException;

public class CaptchaVerifier {
	double confidence = 0.0;
	public boolean verify(HttpServletRequest req, FederationMember idp, String token) throws IOException, InternalErrorException {
		
		if (idp.getCaptchaSecret() == null || idp.getCaptchaSecret().getPassword().isEmpty())
			throw new InternalErrorException("Missing captcha secret for identity provider "+idp.getPublicId());
		
		URL u = new URL("https://www.google.com/recaptcha/api/siteverify");
		HttpURLConnection conn = (HttpURLConnection) u.openConnection();
		conn.setDoInput(true);
		conn.setDoOutput(true);
		conn.setRequestMethod("POST");
		conn.addRequestProperty("Content-type", "application/x-www-form-urlencoded");
		
		conn.connect();
		
		PrintStream out = new PrintStream(conn.getOutputStream());
		
		out.print("secret="+URLEncoder.encode(idp.getCaptchaSecret().getPassword(), "UTF-8"));
		out.print("&");
		out.print("response="+URLEncoder.encode(token, "UTF-8"));
		out.print("&");
		out.print("remoteip="+URLEncoder.encode(req.getRemoteAddr(),"UTF-8"));
		out.close();
		
		JSONObject response = new JSONObject(new JSONTokener(conn.getInputStream()));
		
		if (! response.getBoolean("success"))
			throw new InternalErrorException("Unable to check if the user is or not a human");
		confidence = response.getDouble("score");
		
		return confidence >= (idp.getCaptchaThreshold() == null ? 0.75: idp.getCaptchaThreshold().doubleValue());
	}
	
	public double getConfidence() {
		return confidence;
	}
}