package com.itau.mygod.service;

import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import android.util.Log;


public class HttpUtils 
{  
    /**作为标记在*/
    private static final String TAG = "HttpUtils处理类";
    /**用户识别*/
     public static final String User_Agent="";     

     
     
     private static JSONObject postrequest(String url,List<NameValuePair> params) 
     {
    //在这里可以进行一些处理，如添加时间戳，对参数进行排序等等
        
        try 
        {
            //建立JSONObject对象
             JSONObject object=null;
             //建立HttpClient对象
             HttpClient httpClient=new DefaultHttpClient();
             //设置请求路径        
             HttpPost post=new HttpPost(url);  
             //设置请求参数
             post.setEntity(new UrlEncodedFormEntity(params,"UTF-8"));    
             //设置用户识别
             post.addHeader("User-Agent", User_Agent);
             //建立HttpParams对象
             HttpParams httpParams=post.getParams();
             //设置读取超时
             httpParams.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT,10000);
             //设置请求超时
             httpParams.setParameter(CoreConnectionPNames.SO_TIMEOUT, 10000);
             //发送请求获得回馈
             HttpResponse httpResponse=httpClient.execute(post);
             //取得反馈信息
             HttpEntity httpEntity=httpResponse.getEntity();
             //如果httpEntity不为空
             if(httpEntity!=null )
             {
                 String result=EntityUtils.toString(httpEntity);
                 object=new JSONObject(result);
                 
                 Log.i(TAG, "post请求成功:"+ result);

                 return object;
             }
             else    
             {
                 Log.e(TAG, "post请求失败");
                 return null;
             }
        }
        catch (Exception e) 
        {
        
            Log.i(TAG,e.toString());
            return null;
            
        }
    
     }
     
     private static JSONObject postrequest(String url) 
     {
    //在这里可以进行一些处理，如添加时间戳，对参数进行排序等等
        
        try 
        {
            //建立JSONObject对象
             JSONObject object=null;
             //建立HttpClient对象
             HttpClient httpClient=new DefaultHttpClient();
             //设置请求路径        
             HttpPost post=new HttpPost(url);     
             //设置用户识别
             post.addHeader("User-Agent", User_Agent);
             //建立HttpParams对象
             HttpParams httpParams=post.getParams();
             //设置读取超时
             httpParams.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT,10000);
             //设置请求超时
             httpParams.setParameter(CoreConnectionPNames.SO_TIMEOUT, 10000);
             //发送请求获得回馈
             HttpResponse httpResponse=httpClient.execute(post);
             //取得反馈信息
             HttpEntity httpEntity=httpResponse.getEntity();
             //如果httpEntity不为空
             if(httpEntity!=null )
             {
                 String result=EntityUtils.toString(httpEntity);
                 object=new JSONObject(result);
                 
                 Log.i(TAG, "post请求成功:"+ result);

                 return object;
             }
             else    
             {
                 Log.e(TAG, "post请求失败");
                 return null;
             }
        }
        catch (Exception e) 
        {
        
            Log.i(TAG,e.toString());
            return null;
            
        }
    
     }
         
     /**
     * post方法传参
     * @param url         ip路径
     * @param method      方法名
     * @param params      参数
     * @return
     */
    public static JSONObject post(String url,String method,List<NameValuePair> params) throws Exception 
    {        
        
            return postrequest(url+method, params);        
    }
    
    public static JSONObject post(String url,String method) throws Exception 
    {        
        
            return postrequest(url+method);        
    }
 
}