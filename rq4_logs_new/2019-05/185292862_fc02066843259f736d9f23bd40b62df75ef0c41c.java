package com.hundsun.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class LocalNetworkUtil {

	private static Logger logger = LoggerFactory.getLogger(LocalNetworkUtil.class);
    
	/**
	 * 获取本机的IP地址<br>
	 * 
	 * 
	 * @return String
	 */
    public static String getLocalIPAddress() {
		String ipAddress = null;
		InetAddress inetAddress = null;
		try {
			boolean bFindIP = false;
			Enumeration<NetworkInterface> netInterfaces = (Enumeration<NetworkInterface>) NetworkInterface.getNetworkInterfaces();
			while (netInterfaces.hasMoreElements()) {
				if (bFindIP) {
					break;
				}
				NetworkInterface ni = (NetworkInterface) netInterfaces.nextElement();
				Enumeration<InetAddress> inetAddressEnum = ni.getInetAddresses();
				while (inetAddressEnum.hasMoreElements()) {
					inetAddress = (InetAddress) inetAddressEnum.nextElement();
					if (!inetAddress.isLoopbackAddress() && inetAddress.getHostAddress().matches("(\\d{1,3}\\.){3}\\d{1,3}")) {
						bFindIP = true;
						break;
					}
				}
			}
			if(!bFindIP){
				return ipAddress;
			}
		} catch (Exception e) {
			logger.error("IP地址获取失败:" + e.getMessage());
			e.printStackTrace();
		}
		if (null != inetAddress) {
			ipAddress = inetAddress.getHostAddress();
		}
		return ipAddress;
	}
    
    /**
	 * 获取本机的IP地址<br>
	 * 
	 * 
	 * @return String
	 */
	public static String getLocalMacAddress() {
		String macAddress = null;
		InetAddress inetAddress = null;
		NetworkInterface networkInterface = null;
		try {
			boolean bFindIP = false;
			Enumeration<NetworkInterface> netInterfaces = (Enumeration<NetworkInterface>) NetworkInterface.getNetworkInterfaces();
			while (netInterfaces.hasMoreElements()) {
				if (bFindIP) {
					break;
				}
				networkInterface = (NetworkInterface) netInterfaces.nextElement();
				// 遍历所有inetAddress
				Enumeration<InetAddress> inetAddressEnum = networkInterface.getInetAddresses();
				while (inetAddressEnum.hasMoreElements()) {
					inetAddress = (InetAddress) inetAddressEnum.nextElement();
					if (!inetAddress.isLoopbackAddress() && inetAddress.getHostAddress().matches("(\\d{1,3}\\.){3}\\d{1,3}")) {
						bFindIP = true;
						break;
					}
				}
			}
			if(!bFindIP){
				return macAddress;
			}
		} catch (Exception e) {
			logger.error("IP地址获取失败:" + e.getMessage());
			e.printStackTrace();
		}
		if (null != inetAddress) {
			try {
				macAddress = getMacFromBytes(networkInterface.getHardwareAddress());
			} catch (SocketException e) {
				logger.error("MAC地址获取失败:" + e.getMessage());
				e.printStackTrace();
			}
		}
		return macAddress;
	}
	
	/**
	 * 将MAC地址以十六进制表示<br>
	 * 
	 * 
	 * @param bytes
	 * @return String
	 */
	private static String getMacFromBytes(byte[] bytes) {
		StringBuffer mac = new StringBuffer();
		byte currentByte;
		for (byte b : bytes) {
			currentByte = (byte) ((b & 240) >> 4);
			mac.append(Integer.toHexString(currentByte));
			currentByte = (byte) (b & 15);
			mac.append(Integer.toHexString(currentByte));
		}
		return mac.toString().toUpperCase();
	}
	
	/**
	 * 获取硬盘序列号
	 * @return
	 */
	public static String getHdSerialInfo() {
		String line = "";
		String HdSerial = "";// 定义变量 硬盘序列号
		try {
			Process proces = Runtime.getRuntime().exec("cmd /c dir c:");// 获取命令行参数
			BufferedReader buffreader = new BufferedReader(new InputStreamReader(proces.getInputStream(), "gbk"));
			while ((line = buffreader.readLine()) != null) {
				if (line.indexOf("卷的序列号是 ") != -1) { // 读取参数并获取硬盘序列号
					HdSerial = line.substring(line.indexOf("卷的序列号是 ") + "卷的序列号是 ".length(), line.length());
					break;
				}
			}
		} catch (IOException e) {
		}
		return HdSerial;
	}

	public static void main(String[] args) {
		System.out.println(getLocalIPAddress());
		System.out.println(getLocalMacAddress());

	}
}