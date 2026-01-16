package com.study.common.web;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLContext;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.CookieStore;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.cookie.Cookie;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

/**
 * Http工具，包含：普通http请求工具(使用httpClient进行http,https请求的发送)
 */
public class HttpCommonUtil {
    /**
     * 请求超时时间,默认20000ms
     */
    private int timeout = 20000;

    /**
     * cookie表
     */
    private Map<String, String> cookieMap = new HashMap<>();

    /**
     * http请求头, 包含签名秘钥等
     */
    private String header;

    /**
     * 请求编码(处理返回结果)，默认UTF-8
     */
    private String charset = "UTF-8";

    /**
     * 饿汉单例
     */
    private static final HttpCommonUtil INSTANCE = new HttpCommonUtil();

    private HttpCommonUtil() {
    }

    /**
     * 获取实例
     *
     * @return
     */
    public static HttpCommonUtil getInstance() {
        return INSTANCE;
    }

    /**
     * 清空cookieMap
     */
    public void clearCookieMap() {
        cookieMap.clear();
    }

    public int getTimeout() {
        return timeout;
    }

    /**
     * 设置请求超时时间
     *
     * @param timeout
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String getCharset() {
        return charset;
    }

    /**
     * 设置请求字符编码集
     *
     * @param charset
     */
    public void setCharset(String charset) {
        this.charset = charset;
    }

    /**
     * 字符串转换为map
     */
    public void setCookieMap(String cookie) {
        if (StringUtils.isNotBlank(cookie)) {
            String[] mapStrings = cookie.trim().split("; ");
            for (String mapString : mapStrings) {
                if (StringUtils.isNotBlank(mapString)) {
                    String[] keyValues = mapString.split("=");

                    cookieMap.put(keyValues[0], mapString.substring(keyValues[0].length()));
                }
            }
        }
    }

    public Map<String, String> getCookieMap() {
        return cookieMap;
    }

    /**
     * 执行get请求
     *
     * @param url 请求url
     * @return 响应体的字符串
     * @throws
     */
    public String executeGet(String url) throws Exception {
        HttpGet httpGet = new HttpGet(url);
        httpGet.setHeader("Cookie", convertCookieMapToString());
        httpGet.setConfig(RequestConfig.custom().setSocketTimeout(timeout).setConnectTimeout(timeout).build());
        CloseableHttpClient httpClient = null;
        String str = "";
        try {
            httpClient = HttpClientBuilder.create().build();
            HttpClientContext context = HttpClientContext.create();
            CloseableHttpResponse response = httpClient.execute(httpGet, context);
            getCookiesFromCookieStore(context.getCookieStore());
            int state = response.getStatusLine().getStatusCode();
            if (state == 404) {
                str = "";
            }
            try {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    str = EntityUtils.toString(entity, charset);
                }
            } finally {
                response.close();
            }
        } catch (IOException e) {
            throw e;
        } finally {
            try {
                if (httpClient != null) {
                    httpClient.close();
                }
            } catch (IOException e) {
                throw e;
            }
        }
        return str;
    }

    /**
     * 将网页返回为解析后的文档格式
     *
     * @param html 请求地址
     * @return 取出特殊符号后的html形式
     * @throws Exception
     */
    public Document parseHtmlToDoc(String html) throws Exception {
        return removeHtmlSpace(html);
    }

    /**
     * 解析字符串,去除字符串中的特殊符号
     *
     * @param str
     * @return 去除后的文本
     */
    private Document removeHtmlSpace(String str) {
        Document doc = Jsoup.parse(str);
        String result = doc.html().replace("&nbsp;", "");
        return Jsoup.parse(result);
    }

    /**
     * 执行get请求,返回doc
     *
     * @param url
     * @return
     * @throws Exception
     */
    public Document executeGetAsDocument(String url) throws Exception {
        return parseHtmlToDoc(executeGet(url));
    }

    /**
     * 用https执行get请求,返回doc
     *
     * @param url
     * @return
     * @throws Exception
     */
    public Document executeGetWithSSLAsDocument(String url) throws Exception {
        return parseHtmlToDoc(executeGetWithSSL(url));
    }

    /**
     * 用https执行get请求
     *
     * @param url
     * @return
     * @throws Exception
     */
    public String executeGetWithSSL(String url) throws Exception {
        HttpGet httpGet = new HttpGet(url);
        httpGet.setHeader("Cookie", convertCookieMapToString());
        httpGet.setConfig(RequestConfig.custom().setSocketTimeout(timeout).setConnectTimeout(timeout).build());
        CloseableHttpClient httpClient = null;
        String str = "";
        try {
            httpClient = createSSLInsecureClient();
            HttpClientContext context = HttpClientContext.create();
            CloseableHttpResponse response = httpClient.execute(httpGet, context);
            getCookiesFromCookieStore(context.getCookieStore());
            int state = response.getStatusLine().getStatusCode();
            if (state == 404) {
                str = "";
            }
            try {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    str = EntityUtils.toString(entity, charset);
                }
            } finally {
                response.close();
            }
        } catch (IOException e) {
            throw e;
        } catch (GeneralSecurityException ex) {
            throw ex;
        } finally {
            try {
                if (httpClient != null) {
                    httpClient.close();
                }
            } catch (IOException e) {
                throw e;
            }
        }
        return str;
    }

    /**
     * 执行post请求,返回doc
     *
     * @param url
     * @param params
     * @return
     * @throws Exception
     */
    public Document executePostAsDocument(String url, Map<String, String> params) throws Exception {
        return parseHtmlToDoc(executePost(url, params));
    }

    /**
     * 执行post请求
     *
     * @param url
     * @param paramMap
     * @return
     * @throws Exception
     */
    public String executePost(String url, Map<String, String> paramMap) throws Exception {
        String reStr = "";
        HttpPost httpPost = new HttpPost(url);
        httpPost.setConfig(RequestConfig.custom().setSocketTimeout(timeout).setConnectTimeout(timeout).build());
        httpPost.setHeader("Cookie", convertCookieMapToString());
        List<NameValuePair> paramsRe = new ArrayList<>();
        for (Map.Entry<String, String> entry : paramMap.entrySet()) {
            paramsRe.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
        }
        CloseableHttpClient httpclient = HttpClientBuilder.create().build();
        CloseableHttpResponse response;
        try {
            httpPost.setEntity(new UrlEncodedFormEntity(paramsRe));
            HttpClientContext context = HttpClientContext.create();
            response = httpclient.execute(httpPost, context);
            getCookiesFromCookieStore(context.getCookieStore());
            HttpEntity entity = response.getEntity();
            reStr = EntityUtils.toString(entity, charset);
        } catch (IOException e) {
            throw e;
        } finally {
            httpPost.releaseConnection();
        }
        return reStr;
    }

    /**
     * 用https执行post请求,返回doc
     *
     * @param url
     * @param params
     * @return
     * @throws Exception
     */
    public Document executePostWithSSLAsDocument(String url, Map<String, String> params) throws Exception {
        return parseHtmlToDoc(executePostWithSSL(url, params));
    }

    /**
     * 用https执行post请求
     *
     * @param url
     * @param params
     * @return
     * @throws Exception
     */
    public String executePostWithSSL(String url, Map<String, String> params) throws Exception {
        String re = "";
        HttpPost post = new HttpPost(url);
        List<NameValuePair> paramsRe = new ArrayList<>();
        for (String key : params.keySet()) {
            paramsRe.add(new BasicNameValuePair(key, params.get(key)));
        }
        post.setHeader("Cookie", convertCookieMapToString());
        post.setConfig(RequestConfig.custom().setSocketTimeout(timeout).setConnectTimeout(timeout).build());
        CloseableHttpResponse response;
        try {
            CloseableHttpClient httpClientRe = createSSLInsecureClient();
            HttpClientContext contextRe = HttpClientContext.create();
            post.setEntity(new UrlEncodedFormEntity(paramsRe));
            response = httpClientRe.execute(post, contextRe);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                re = EntityUtils.toString(entity, charset);
            }
            getCookiesFromCookieStore(contextRe.getCookieStore());
        } catch (Exception e) {
            throw e;
        }
        return re;
    }

    /**
     * 发送JSON格式body的POST请求
     *
     * @param url      地址
     * @param jsonBody json body
     * @return
     * @throws Exception
     */
    public String executePostWithJson(String url, String jsonBody) throws Exception {
        return executePostWithJsonAndSSL(url, jsonBody, null);
    }

    /**
     * 带请求头发送JSON格式body的POST请求
     *
     * @param url      地址
     * @param jsonBody 请求参数
     * @param headerMap 请求头map
     * @return 响应内容
     * @throws Exception 异常
     */
    public String executePostWithJsonAndSSL(String url, String jsonBody, Map<String, String> headerMap) throws Exception {
        String re = "";
        HttpPost post = new HttpPost(url);

        if (MapUtils.isNotEmpty(headerMap)) {
            List<Header> list = new ArrayList<>();
            for (Map.Entry<String, String> entry : headerMap.entrySet()) {
                list.add(new BasicHeader(entry.getKey(), entry.getValue()));
            }
            post.setHeaders(list.toArray(new Header[]{}));
        } else {
            post.setHeader("Cookie", convertCookieMapToString());
        }
        post.setConfig(RequestConfig.custom().setSocketTimeout(timeout).setConnectTimeout(timeout).build());
        CloseableHttpResponse response;
        try {
            CloseableHttpClient httpClientRe = createSSLInsecureClient();
            HttpClientContext contextRe = HttpClientContext.create();
            post.setEntity(new StringEntity(jsonBody, ContentType.APPLICATION_JSON));
            response = httpClientRe.execute(post, contextRe);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                re = EntityUtils.toString(entity, charset);
            }
            getCookiesFromCookieStore(contextRe.getCookieStore());
        } catch (Exception e) {
            throw e;
        }
        return re;
    }

    /**
     * 发送JSON格式body的SSL POST请求
     *
     * @param url      地址
     * @param jsonBody
     * @return
     * @throws Exception
     */
    public String executePostWithJsonAndSSL(String url, String jsonBody) throws Exception {
        String re = "";
        HttpPost post = new HttpPost(url);
        post.setHeader("Cookie", convertCookieMapToString());
        post.setConfig(RequestConfig.custom().setSocketTimeout(timeout).setConnectTimeout(timeout).build());
        CloseableHttpResponse response;
        try {
            CloseableHttpClient httpClientRe = createSSLInsecureClient();
            HttpClientContext contextRe = HttpClientContext.create();
            post.setEntity(new StringEntity(jsonBody, ContentType.APPLICATION_JSON));
            response = httpClientRe.execute(post, contextRe);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                re = EntityUtils.toString(entity, charset);
            }
            getCookiesFromCookieStore(contextRe.getCookieStore());
        } catch (Exception e) {
            throw e;
        }
        return re;
    }

    private void getCookiesFromCookieStore(CookieStore cookieStore) {
        List<Cookie> cookies = cookieStore.getCookies();
        for (Cookie cookie : cookies) {
            cookieMap.put(cookie.getName(), cookie.getValue());
        }
    }

    /**
     * 截取key=value形式
     *
     * @return
     */
    private String convertCookieMapToString() {
        String cookie = "";
        for (Map.Entry<String, String> entry : cookieMap.entrySet()) {
            cookie += (entry.getKey() + "=" + entry.getValue() + "; ");
        }

        //;后有一个空格,不截取它们所以-2
        if (cookieMap.size() > 0) {
            cookie = cookie.substring(0, cookie.length() - 2);
        }
        return cookie;
    }

    /**
     * 创建 SSL连接
     *
     * @return http连接实例
     * @throws GeneralSecurityException
     */
    private CloseableHttpClient createSSLInsecureClient() throws GeneralSecurityException {
        try {
            SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, (chain, authType) -> true).build();
            SSLConnectionSocketFactory sslConnectionSocketFactory = new SSLConnectionSocketFactory(sslContext,
                    (s, sslContextL) -> true);
            return HttpClients.custom().setSSLSocketFactory(sslConnectionSocketFactory).build();
        } catch (GeneralSecurityException e) {
            throw e;
        }
    }
}