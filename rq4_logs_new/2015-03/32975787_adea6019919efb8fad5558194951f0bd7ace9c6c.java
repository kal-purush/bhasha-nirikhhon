package com.broadcast.cache;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.eclipse.wb.swt.ResourceUtils;

public abstract class CacheUtils {

	private static Map<String, Object> cache = null;

	public synchronized static void add(String key, List<?> list) {
		if (cache == null) {
			readCache();
		}
		cache.put(key, list);
		write();
	}

	public synchronized static void add(String key, Serializable serializable) {
		if (cache == null) {
			readCache();
		}
		cache.put(key, serializable);
		write();
	}

	public synchronized static <T> T get(String key) {
		if (cache == null) {
			readCache();
		}
		return (T) cache.get(key);
	}

	protected synchronized static void readCache() {
		try {
			File file = new File(ResourceUtils.getResource("props.cache"));
			if (!file.exists()) {
				file.createNewFile();
				cache = new LinkedHashMap<String, Object>();
				return;
			}
			if (FileUtils.readFileToByteArray(file).length == 0) {
				if (cache == null) {
					cache = new LinkedHashMap<String, Object>();
				}
				return;
			}
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(
					file));
			cache = (Map<String, Object>) ois.readObject();
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	protected synchronized static void write() {
		ObjectOutputStream oo = null;
		try {
			File file = new File(ResourceUtils.getResource("props.cache"));
			if (!file.exists()) {
				file.createNewFile();
			}
			oo = new ObjectOutputStream(new FileOutputStream(file));
			oo.writeObject(cache);
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		} finally {
			if (oo != null) {
				try {
					oo.close();
				} catch (IOException e) {
				}
			}
		}
	}

	public final static String _第一名_姓名 = "_第一名_姓名";
	public final static String _第一名_数量 = "_第一名_数量";
	public final static String _第二名_姓名 = "_第二名_姓名";
	public final static String _第二名_数量 = "_第二名_数量";
	public final static String _第三名_姓名 = "_第三名_姓名";
	public final static String _第三名_数量 = "_第三名_数量";
	public final static String _第四名_姓名 = "_第四名_姓名";
	public final static String _第四名_数量 = "_第四名_数量";
	public final static String _第五名_姓名 = "_第五名_姓名";
	public final static String _第五名_数量 = "_第五名_数量";
	public final static String _第六名_姓名 = "_第六名_姓名";
	public final static String _第六名_数量 = "_第六名_数量";
	public final static String _第七名_姓名 = "_第七名_姓名";
	public final static String _第七名_数量 = "_第七名_数量";
	public final static String _第八名_姓名 = "_第八名_姓名";
	public final static String _第八名_数量 = "_第八名_数量";
	public final static String _第九名_姓名 = "_第九名_姓名";
	public final static String _第九名_数量 = "_第九名_数量";
	public final static String _第十名_数量 = "_第十名_数量";
	public final static String _总数量 = "_总数量";
	public final static String _总人数 = "_总人数";
	public final static String _屏幕 = "_屏幕";
	public final static String _订单 = "_订单";
}