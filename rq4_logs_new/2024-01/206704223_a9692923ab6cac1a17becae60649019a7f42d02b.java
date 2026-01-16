package com.coding.challenge.hash.table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Codec {
	private final List<String> tinyUrlToLongUrl = new ArrayList<>();
	private final Map<String, Integer> longUrlToTinyUrl = new HashMap<>();
	int counter = 0;

	public Codec() {
	}

	public static void main(String[] args) {
		final Codec codec = new Codec();
		final String longUrl = "https://leetcode.com/problems/design-tinyurl";
		assert longUrl.equals(codec.decode(codec.encode(longUrl)));
	}

	// Encodes a URL to a shortened URL.
	public String encode(String longUrl) {
		if (longUrlToTinyUrl.containsKey(longUrl))
			return longUrlToTinyUrl.get(longUrl) + "";
		tinyUrlToLongUrl.add(longUrl);
		longUrlToTinyUrl.put(longUrl, counter);
		final String tinyUrl = counter + "";
		counter = counter + 1;
		return tinyUrl;
	}

	// Decodes a shortened URL to its original URL.
	public String decode(String shortUrl) {
		return tinyUrlToLongUrl.get(Integer.parseInt(shortUrl));
	}
}