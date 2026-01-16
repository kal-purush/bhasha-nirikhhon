package com.kain.hap.proxy.config;



import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import javax.jmdns.JmDNS;
import javax.jmdns.ServiceInfo;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.hash.Hashing;
import com.google.common.primitives.Bytes;
import com.kain.hap.proxy.mdns.MulticastDNSDiscoveryService;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class MDNSConfig {
	
	@Value("${server.port}")
	private int accessoryPort;
	@Value("${accessory.identifier}")
	private String accessoryId;
	@Value("${accessory.model}")
	private String accessoryModel;
	@Value("${accessory.configuration.number}")
	private String accessoryCN;
	@Value("${accessory.state.number}")
	private String accessorySN;
	@Value("${accessory.protocol.version}")
	private String accessoryProtocolVer;
	@Value("${accessory.status.flag}")
	private String accessoryStatusFlag;
	@Value("${accessory.feature.flag}")
	private String accessoryFeatureFlag;
	@Value("${accessory.category.identifier}")
	private String accessoryCategoryId;
	
	@Value("${accessory.setup.identifier}")
	private String accessorySetupId;
	@Value("${accessory.setup.code}")
	private String setupCode;
	
	
	private static final String HAP_TYPE = "_hap._tcp.local."; 
	

	@Bean
	public JmDNS jmdns() {
		try {
			JmDNS jmdns = JmDNS.create(InetAddress.getLocalHost());
			jmdns.registerService(serviceInfo());
			jmdns.addServiceListener(HAP_TYPE, disoveryService());
			return jmdns;
		} catch (IOException e) {
			log.error("jmDNS wasn't create correctly", e);
			return null;
		}
	}
	
	@Bean
	public ServiceInfo serviceInfo() {
		return ServiceInfo.create(HAP_TYPE, accessoryModel, accessoryPort, 0, 0, accessoryDNSProp());
	}
	
	@Bean
	public MulticastDNSDiscoveryService disoveryService() {
		return new MulticastDNSDiscoveryService();
	}
	
	@Bean 
	public Map<String, String> accessoryDNSProp(){
		Map<String, String> map = new HashMap<>();
		map.put("md", accessoryModel); // Model name of the accessory (e.g. "Device1,1"). Required.
		map.put("id", accessoryId); // Device ID (Device ID (page 36)) of the accessory. The Device ID must
											// be formatted as "XX:XX:XX:XX:XX:XX", where "XX" is a hexadecimal
											// string representing a byte. Required.
		map.put("c#", accessoryCN); // Current configuration number. Required.
		map.put("s#", accessorySN); // Current state number. Required.
		map.put("pv", accessoryProtocolVer); // Protocol version string <major>.<minor> (e.g. "1.0"). Required if value is
								// not "1.0".
		map.put("sf", accessoryStatusFlag);
		map.put("ff", accessoryFeatureFlag); // Feature flags (e.g. "0x3" for bits 0 and 1). Required if non-zero. // should be 0
		map.put("ci", accessoryCategoryId);
		map.put("sh", getSetupHash());
		return map;
	}

	private String getSetupHash() {
		byte[] sh = Bytes.concat(accessorySetupId.getBytes(), accessoryId.getBytes());
		byte[] hashed = Hashing.sha512().hashBytes(sh).asBytes();
		return Base64.getEncoder().encodeToString(Arrays.copyOf(hashed, 4));
		
	}

}