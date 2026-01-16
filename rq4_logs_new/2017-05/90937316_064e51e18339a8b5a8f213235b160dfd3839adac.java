package com.mock.wifiserver;

import io.netty.channel.Channel;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DeviceManager {
	
	private static final ConcurrentMap<String, Channel> devices = new ConcurrentHashMap<String, Channel>();
	
	public static void addDevice(String mac,Channel channel) {
		devices.put(mac, channel);
	}
	
	public static Channel channel(String mac) {
		return devices.get(mac);
	}
}