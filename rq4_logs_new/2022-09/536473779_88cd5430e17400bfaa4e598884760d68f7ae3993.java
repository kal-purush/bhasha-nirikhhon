package com.kain.hap.proxy.service;

import java.security.SecureRandom;

import com.kain.hap.proxy.srp.Group;

import lombok.Getter;

//Client
public class DeviceSession {
	@Getter
	private final byte[] salt;
	private final byte[] privateKey; // a
	@Getter
	private byte[] publicKey; // A
	
	private static final int SALT_LENGTH = 16;
	private static final int KEY_LENGTH = 64;
	private static final SecureRandom random = new SecureRandom();
	
	private byte[] generate(int length) {
		byte[] generated = new byte[length];
		random.nextBytes(generated);
		return generated;
	}
	
	public DeviceSession() {
		salt = generate(SALT_LENGTH);
		privateKey = generate(KEY_LENGTH);
		publicKey = SrpCalculation.generateClientPublic(Group.G_3072_BIT, privateKey);
	}

}