package com.sturents.api;

import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.digest.Md5Crypt;

public class MessageHasher {

    public String hashThis(String plaintext) throws NoSuchAlgorithmException {
	return Md5Crypt.md5Crypt(plaintext.getBytes());
    }
}