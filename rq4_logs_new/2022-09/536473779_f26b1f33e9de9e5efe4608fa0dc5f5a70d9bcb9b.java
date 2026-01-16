package com.kain.hap.proxy.tlv.serialize;

import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.primitives.Bytes;

public class TlvWrapper {
	private Map<Byte, byte[]> dataMap = Maps.newHashMap();
	
	public void add(byte type, byte[] array) {
		if (dataMap.containsKey(type)) {
			dataMap.put(type, Bytes.concat(dataMap.get(type), array));
		} else {
			dataMap.put(type, array);
		}
	}
	
	public byte[] get(byte type) {
		return dataMap.get(type);
	}
	
	public byte getOne(byte type) {
		return dataMap.get(type)[0];
	}
	
	public boolean contains(byte type) {
		return dataMap.containsKey(type);
	}

}