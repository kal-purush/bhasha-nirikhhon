package org.apache.zeppelin.iginx.util;

import java.io.IOException;
import java.util.List;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.zeppelin.iginx.IginxInterpreter8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(IginxInterpreter8.class);
  private static final int CONNECTION_TIMEOUT = 5000;
  private static final int SOCKET_TIMEOUT = 5000;

  public static String sendGet(String url) {
    CloseableHttpClient httpClient = getHttpClientWithConfig(CONNECTION_TIMEOUT, SOCKET_TIMEOUT);
    HttpGet httpGet = new HttpGet(url);
    CloseableHttpResponse response = null;
    try {
      response = httpClient.execute(httpGet);
      HttpEntity entity = response.getEntity();
      if (entity != null) {
        return EntityUtils.toString(entity);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      try {
        if (response != null) {
          response.close();
        }
        httpClient.close();
      } catch (IOException e) {
        LOGGER.error("HttpUtil sendGet error", e);
      }
    }

    return null;
  }

  // 发送 POST 请求
  public static String sendPost(String url, List<NameValuePair> params) {
    CloseableHttpClient httpClient = getHttpClientWithConfig(CONNECTION_TIMEOUT, SOCKET_TIMEOUT);
    HttpPost httpPost = new HttpPost(url);
    CloseableHttpResponse response = null;
    try {
      if (params != null && !params.isEmpty()) {
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(params, "UTF-8");
        httpPost.setEntity(entity);
      }
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      if (responseEntity != null) {
        return EntityUtils.toString(responseEntity);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      try {
        if (response != null) {
          response.close();
        }
        httpClient.close();
      } catch (IOException e) {
        LOGGER.error("HttpUtil sendPost error", e);
      }
    }
    return null;
  }

  // 设置请求超时时间等配置
  public static CloseableHttpClient getHttpClientWithConfig(int connectTimeout, int socketTimeout) {
    RequestConfig requestConfig =
        RequestConfig.custom()
            .setConnectTimeout(connectTimeout)
            .setSocketTimeout(socketTimeout)
            .build();

    return HttpClients.custom().setDefaultRequestConfig(requestConfig).build();
  }
}