package com.colonycount.cklab.asynctask;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.util.Log;

import com.colonycount.cklab.asynctask.base.AsyncTaskCompleteListener;
import com.colonycount.cklab.asynctask.base.AsyncTaskPayload;
import com.colonycount.cklab.asynctask.base.BaseAsyncTask;

public class GetImgDetailAsyncTask extends BaseAsyncTask {
	private String imgId;
	
	public GetImgDetailAsyncTask(Context context, String progressDialogTitle,
			String progressDialogMsg,
			AsyncTaskCompleteListener<Boolean> listener, Class<?> cls,
			boolean showDialog, String imgId) {
		super(context, progressDialogTitle, progressDialogMsg, listener, cls, showDialog);
		this.imgId = imgId;
	}

	@Override
	protected AsyncTaskPayload doInBackground(AsyncTaskPayload... params) {
		AsyncTaskPayload payload = new AsyncTaskPayload();

		String postUrl = "http://140.112.26.221/~master11360/colony%20count/php/db_connect.php";
		String imgUrl = null;
		HttpPost httpRequest = new HttpPost(postUrl);
		List<NameValuePair> reqParams = new ArrayList<NameValuePair>();
		addRequest(reqParams);
		
		try {
			httpRequest.setEntity(new UrlEncodedFormEntity(reqParams, HTTP.UTF_8));
			HttpResponse httpResponse = new DefaultHttpClient().execute(httpRequest);
			
			Log.d("test4", "status code = " + httpResponse.getStatusLine().getStatusCode());
			if(httpResponse.getStatusLine().getStatusCode() == 200){
				String strResult = EntityUtils.toString(httpResponse.getEntity());
				
				JSONObject jsonObject = new JSONObject(strResult);
				String status = jsonObject.getString("status");
				
				if(status.equals("success")){
					JSONObject resultObj = jsonObject.getJSONObject("result");
					payload.putValue("result", "success");
					payload.setImageInfoObj(resultObj);
					imgUrl = resultObj.getString("path");
				} else if(status.equals("error")){
					payload.putValue("result", "error");
				}
			} else {
				payload.putValue("result", "error");
				payload.putValue("msg", "httpResponse status code = " + httpResponse.getStatusLine().getStatusCode());
			}
			
			// get images from urls
			imgUrl = "http://140.112.26.221/~master11360/colony%20count/php/" + imgUrl;
			Bitmap bmImg = null;
			try {
			    HttpURLConnection conn = (HttpURLConnection) new URL(imgUrl).openConnection();
			    conn.setDoInput(true);
			    conn.connect();
			    InputStream is = conn.getInputStream();
			    bmImg = BitmapFactory.decodeStream(is);
		    } catch (IOException e) {
		        e.printStackTrace();
	        }
			
			if(bmImg != null)
				payload.setRawImg(bmImg);
		} catch(Exception e) {
//			result.putValue("result", "error");
		}
		
		return payload;
	}
	
	public void addRequest(List<NameValuePair> reqParams){
		reqParams.add(new BasicNameValuePair("get_img_detail", "true"));
		reqParams.add(new BasicNameValuePair("img_id", imgId));
	}
}