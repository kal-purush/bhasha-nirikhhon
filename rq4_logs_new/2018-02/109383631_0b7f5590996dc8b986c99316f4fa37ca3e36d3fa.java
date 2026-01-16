package rbccps.smartcity.IDEAM.registerapi.lora;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import rbccps.smartcity.IDEAM.registerapi.parser.createEntityJSONParser;
import rbccps.smartcity.IDEAM.registerapi.parser.entity;
import rbccps.smartcity.IDEAM.urls.URLs;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class loraServerConfiguration {

	static JsonObject response_jsonObject;
	static String jwtKey;
	static String _url;
	static String _value;
	static String response;
	static loraserverConfigurationParser serverConfigurationParser_LoRa;
	static String credentials;
	static JsonObject request;

	static TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {

		public java.security.cert.X509Certificate[] getAcceptedIssuers() {
			return null;
		}

		public void checkClientTrusted(
				java.security.cert.X509Certificate[] certs, String authType) {
			// No need to implement.
		}

		public void checkServerTrusted(
				java.security.cert.X509Certificate[] certs, String authType) {
			// No need to implement.
		}
	} };

	public static String getJWTKey() {

		_url = URLs.getLoraserverJWTURL();
		response = null;
		request = new JsonObject();
		System.out
				.println("+++++++++++In create LoRa Server get JWT Key Block+++++++++++");
		serverConfigurationParser_LoRa = new loraserverConfigurationParser();

		try {

			System.out.println("In get JWT Key ");
			SSLContext sc = SSLContext.getInstance("SSL");
			sc.init(null, trustAllCerts, new java.security.SecureRandom());

			URL url = new URL(_url);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			((HttpsURLConnection) conn).setDefaultSSLSocketFactory(sc
					.getSocketFactory());

			request.addProperty("username", loraserverConfigurationFields
					.getUsername().replaceAll("^\"|\"$", ""));
			request.addProperty("password", loraserverConfigurationFields
					.getPassword().replaceAll("^\"|\"$", ""));

			credentials = request.toString();
			byte[] postDataBytes = credentials.getBytes("UTF-8");

			conn.setRequestMethod("POST");
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setRequestProperty("Accept", "application/json");

			conn.setRequestProperty("Content-Length",
					String.valueOf(postDataBytes.length));
			conn.setDoOutput(true);
			conn.setDoInput(true);

			conn.getOutputStream().write(postDataBytes);

			Reader in = new BufferedReader(new InputStreamReader(
					conn.getInputStream(), "UTF-8"));
			StringBuilder sb = new StringBuilder();
			for (int c; (c = in.read()) >= 0;)
				sb.append((char) c);
			response = sb.toString();
			System.out.println(response);

			try {
				JSONObject jwtKeyjsonObj = new JSONObject(response);
				jwtKey = (String) jwtKeyjsonObj.get("jwt");
				System.out.println(jwtKey);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

			response_jsonObject = new JsonObject();
			response_jsonObject.addProperty("Registration", "failure");
			response_jsonObject.addProperty("Reason",
					"LoRa server error on JWT Key");

			System.out.println("--------------");
			System.out.println(response_jsonObject.toString());
			System.out.println("--------------");

			System.out
					.println("+++++++++++In get JWT Key  catch Block+++++++++++"
							+ e.toString());
			response = response_jsonObject.toString();
		} catch (NoSuchAlgorithmException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (KeyManagementException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return response;

		// Add a FLAG to process the Registration further

	}

	public static String registerLoRaEntity(String entityID) {

		_url = URLs.getLoraserverURL();
		response = null;
		System.out.println("+++++++++++In create LoRa Entity Block+++++++++++");

		try {
			System.out.println("In get LoRa Onboarding ");
						
			URL url = new URL(_url);

			SSLContext sc = SSLContext.getInstance("SSL");
			sc.init(null, trustAllCerts, new java.security.SecureRandom());
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			((HttpsURLConnection) conn).setDefaultSSLSocketFactory(sc
					.getSocketFactory());

			String _postData;
			// Create a structured JSON as per the requirement
		
			//loraserverConfigurationParser.serverConfiguration_configuration_jsonObject.remove("appKey");
			//loraserverConfigurationParser.serverConfiguration_configuration_jsonObject.addProperty("appKey", entity.getEntityapikey().toString().replaceAll("^\"|\"$", ""));
			System.out.println("Replaced appKey in LoRa POST data");
			_postData = loraserverConfigurationParser.serverConfiguration_configuration_jsonObject.toString();
			
			System.out.println("+++++++++++In LoRa Entity try Block+++++++++++"
					+ "\n" + _postData.toString() + "\n");

			byte[] postDataBytes = _postData.toString().getBytes("UTF-8");

			
			conn.setRequestProperty("Grpc-Metadata-Authorization", jwtKey); // Get
																			// JWT
																			// Key
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setRequestProperty("Accept", "application/json");

			conn.setRequestProperty("Content-Length",
					String.valueOf(postDataBytes.length));
			conn.setDoOutput(true);
			conn.setDoInput(true);

			conn.getOutputStream().write(postDataBytes);

			Reader in = new BufferedReader(new InputStreamReader(
					conn.getInputStream(), "UTF-8"));
			StringBuilder sb = new StringBuilder();
			for (int c; (c = in.read()) >= 0;)
				sb.append((char) c);
			response = sb.toString();
			System.out.println("In Onboard LoRa Entity");
			System.out.println(response);
			System.out.println("In Onboard LoRa Entity");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

			response_jsonObject = new JsonObject();
			response_jsonObject.addProperty("Registration", "failure");
			response_jsonObject
					.addProperty("Reason", "Cannot Onboard LoRa device in Server. POST Error.");

			System.out.println("--------------");
			System.out.println(response_jsonObject.toString());
			System.out.println("--------------");

			System.out
					.println("+++++++++++In Onboarding catch Block+++++++++++"
							+ e.toString());
			response = response_jsonObject.toString();

		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeyManagementException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return response;

		// Add a FLAG to process the Registration further
	}

}