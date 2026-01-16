package com.soffid.iam.addons.federation.api;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import com.soffid.iam.api.Password;

public class Digest implements Serializable{
	private String digestString;

	public Digest(String s) throws NoSuchAlgorithmException {
		MessageDigest md = MessageDigest.getInstance("SHA-256");
		byte digest[] = md.digest(s.getBytes(StandardCharsets.UTF_8));
		this.digestString = "{SHA-256}"+Base64.getEncoder().encodeToString(digest);
	}
	
	public String toString() {
		return digestString;
	}
	
	protected Digest() {}
	
	public static Digest decode (String s) {
		if (s == null)
			return null;
		if (!s.startsWith("{") && Password.decode(s).getPassword().isEmpty())
			return null;
		Digest d = new Digest();
		d.digestString = s;
		return d;
	}
	
	public boolean validate (String s) {
		if (digestString == null) return s == null || s.isEmpty();
		else {
			if (digestString.startsWith("{") && digestString.contains("}")) {
				int i = digestString.indexOf("}");
				String algorithm = digestString.substring(1, i);
				String payload = digestString.substring(i+1);
				MessageDigest md;
				try {
					md = MessageDigest.getInstance(algorithm);
					byte digest[] = md.digest(s.getBytes(StandardCharsets.UTF_8));
					return Base64.getEncoder().encodeToString(digest).equals(payload);
				} catch (NoSuchAlgorithmException e) {
					return false;
				}
			} else {
				return Password.decode(digestString).getPassword().equals(s);
			}
		}
	}
}