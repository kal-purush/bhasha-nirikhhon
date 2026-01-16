package com.stupidwind.blog.utils;

import java.util.UUID;

/**
 * @author: StupidWind
 * @date: 2019/9/9 20:35
 * @description: UUID生成工具类
 * @ver: 1.0.0
 */
public class UUIDUtils {

	public static String getUUID() {
		return UUID.randomUUID().toString().replace("-", "");
	}

	public static String getUUID(String prefix) {
		String uuid = getUUID();
		return prefix + uuid.substring(1, uuid.length() - 1);
	}

	public static String getUUID(String prefix, int len) {
		return getUUID(prefix).substring(0, len);
	}

	public static String getDatabaseId(String prefix) {
		return getUUID(prefix, 16);
	}

}