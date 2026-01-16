package com.kain.hap.proxy.service;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;

import com.google.common.hash.Hashing;
import com.google.common.primitives.Bytes;

public abstract class SrpSession {
	protected static final int SALT_LENGTH = 16;
	protected static final int KEY_LENGTH = 64;
	protected static final SecureRandom random = new SecureRandom();
	
	protected byte[] generate(int length) {
		byte[] generated = new byte[length];
		random.nextBytes(generated);
		return generated;
	}
	

}