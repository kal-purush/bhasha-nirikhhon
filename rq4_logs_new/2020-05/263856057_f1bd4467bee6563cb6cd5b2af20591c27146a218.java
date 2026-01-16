package com.jdh.fuhsi.portal.util;

import com.alibaba.fastjson.JSON;
import com.jdh.fuhsi.portal.model.common.req.RequestHead;
import com.jdh.log.LogTools;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;


/**
 * @description 银企直连接口工具类
 * @date 2018/03/24
 *
 */
public class BankEntDirectUtil {
	/**
	 * 请求信息转为json字符串
	 * 
	 * @param head
	 * @param body
	 * @return
	 */
	public static <T> String decorateRequest(RequestHead head, Object body) {
		Map<String, Object> request = new HashMap<String, Object>();
		request.put("head", head);
		request.put("body", body);
		return JSON.toJSONString(request);
	}

	/**
	 * @description 发送请求 无加密
	 * @param request
	 * @param url
	 * @return 
	 * 
	 */
	public static String sendRequest(String request, String url) {
		PrintWriter pw = null;
		BufferedReader in = null;
		String result = "";
		HttpURLConnection conn = null;
		try {
			URL remoteUrl = new URL(url);
			// 打开连接
			conn = (HttpURLConnection) remoteUrl.openConnection();
			// 设置属性
			conn.setDoOutput(true);
			conn.setDoInput(true);
			// 请求类型
			conn.setRequestMethod("POST");
			conn.setUseCaches(false);
			// 字符编码
			conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");
			// 建立链接
			conn.connect();
//			pw = new PrintWriter(conn.getOutputStream());
			pw = new PrintWriter(new OutputStreamWriter(conn.getOutputStream(),"UTF-8"));
			pw.print(request);
			pw.flush();
			// 定义输入流读取url响应
			in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
			String line;
			while ((line = in.readLine()) != null) {
				result += line;
			}
		} catch (Exception e) {
			System.err.println("===============请求异常================");
			LogTools.error("发送请求 无加密失败：{}", e.getMessage());
		} finally {
			if (pw != null) {
				pw.close();
			}
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					System.err.println("===============关闭输入流异常================");
					LogTools.error("发送请求 无加密失败：{}", e.getMessage());
				}
			}
			if (conn != null) {
				conn.disconnect();
			}
		}
		return result;
	}
	
	

}