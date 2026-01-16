package com.sturents.api;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import com.sturents.api.SendHouses;
import java.security.NoSuchAlgorithmException;

public class Cli {
	public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
		String json = readFile("data.json");

		String landlord_id = args[0];
		String api_key = args[1];
		
		System.out.println(landlord_id);

		String response = SendHouses.run(landlord_id, api_key, json);

        System.out.println(response);
	}

	static String readFile(String path) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		
		return new String(encoded, "UTF-8");
	}
}