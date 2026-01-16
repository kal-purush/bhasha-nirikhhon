package com.truesender.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

public class Trusender {
	
	
	public static JSONObject sendEvent(String authToken, String templateName, String emailAddress, String properties) {
		final JSONObject jo = new JSONObject();
		CloseableHttpClient httpclient = null;
        try {
        	httpclient = HttpClients.createDefault();
        	HttpPost httpPost = new HttpPost("https://api.trusender.com/v1/sendEvent");
        	httpPost.addHeader("Accept", "application/json");
        	httpPost.addHeader("Content-Type", "application/json");
        	
        	List<NameValuePair> nvps = new ArrayList<NameValuePair> ();
        	nvps.add(new BasicNameValuePair("auth_token", authToken));
        	nvps.add(new BasicNameValuePair("email", emailAddress));
        	nvps.add(new BasicNameValuePair("template_name", templateName));
        	nvps.add(new BasicNameValuePair("properties", properties));
        	httpPost.setEntity(new UrlEncodedFormEntity(nvps));

            System.out.println("Executing request " + httpPost.getRequestLine());

            // Create a custom response handler
            ResponseHandler<String> responseHandler = new ResponseHandler<String>() {

                public String handleResponse(final HttpResponse response) throws ClientProtocolException, IOException {
                    int status = response.getStatusLine().getStatusCode();
                    if (status >= 200 && status < 300) {
                    	jo.put("success", true);
                    	jo.put("message", "Successfully added event!");
                        HttpEntity entity = response.getEntity();
                        return entity != null ? EntityUtils.toString(entity) : null;
                    } else if (status == 422) {
                    	jo.put("success", true);
                    	jo.put("message", "Successfully added event!");
                    	HttpEntity entity = response.getEntity();
                        return entity != null ? EntityUtils.toString(entity) : null;
                    } else {
                        throw new ClientProtocolException("Unexpected response status: " + status);
                    }
                }

            };
            String responseBody = httpclient.execute(httpPost, responseHandler);
            System.out.println("----------------------------------------");
            System.out.println(responseBody);
            
        } catch(Exception ex) {
        	jo.put("success", false);
        	jo.put("message", "SendEvent : Email delivery failed "+ex.getMessage());
        } finally {
        	try {
        		if(httpclient!=null)
        			httpclient.close();
			} catch (Exception e) {
				System.out.println("Error while closing http client "+e.getMessage());
			}
        }
		return jo;
	}
	
	public static JSONObject sendEmail(String authToken, String templateName, String toAddress, String dataMapping) {
		final JSONObject jo = new JSONObject();
		CloseableHttpClient httpclient = null;
        try {
        	httpclient = HttpClients.createDefault();
        	HttpPost httpPost = new HttpPost("https://api.trusender.com/v1/sendEmail");
        	List<NameValuePair> nvps = new ArrayList<NameValuePair> ();
        	nvps.add(new BasicNameValuePair("auth_token", authToken));
        	nvps.add(new BasicNameValuePair("email", toAddress));
        	nvps.add(new BasicNameValuePair("template_name", templateName));
        	nvps.add(new BasicNameValuePair("data_mapping", dataMapping));
        	httpPost.setEntity(new UrlEncodedFormEntity(nvps));

            System.out.println("Executing request " + httpPost.getRequestLine());

            // Create a custom response handler
            ResponseHandler<String> responseHandler = new ResponseHandler<String>() {

                public String handleResponse(final HttpResponse response) throws ClientProtocolException, IOException {
                    int status = response.getStatusLine().getStatusCode();
                    if (status >= 200 && status < 300) {
                    	jo.put("success", true);
                    	jo.put("message", "Successfully sent the email!");
                        HttpEntity entity = response.getEntity();
                        return entity != null ? EntityUtils.toString(entity) : null;
                    } else if (status == 422) {
                    	jo.put("success", true);
                    	jo.put("message", "Successfully sent the email!");
                    	HttpEntity entity = response.getEntity();
                        return entity != null ? EntityUtils.toString(entity) : null;
                    } else {
                        throw new ClientProtocolException("Unexpected response status: " + status);
                    }
                }

            };
            String responseBody = httpclient.execute(httpPost, responseHandler);
            System.out.println("----------------------------------------");
            System.out.println(responseBody);
            
        } catch(Exception ex) {
        	jo.put("success", false);
        	jo.put("message", "SendEmail : Oops Something went wrong!! "+ex.getMessage());
        } finally {
        	try {
        		if(httpclient!=null)
        			httpclient.close();
			} catch (Exception e) {
				System.out.println("Error while closing http client "+e.getMessage());
			}
        }
		return jo;
	
		
	}
	
}