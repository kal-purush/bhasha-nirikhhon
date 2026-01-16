package com.html5parser.parser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.w3c.dom.Document;

/**
 * Hello world!
 *
 */
public class HTML5Parser {
	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("Two parameters are required");
			return;
		}

		Document doc = null;
		String html = null;

		switch (args[0]) {
		case "-f":
			html = readFile(args[1]);
			if (html == null)
				return;
			break;
		case "-s":
			html = args[1];
			break;
		default:
			System.out.println("Invalid option");
			return;
		}
		try {
			Parser parser = new Parser();
			doc = parser.parse(html);
			System.out.println(Serializer.toHtml5libFormat(doc));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static String readFile(String path) {
		StringBuilder sb = new StringBuilder();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(path));
			String line;
			if((line = br.readLine()) != null)
				sb.append(line);
			while ((line = br.readLine()) != null) {
				sb.append(System.getProperty("line.separator"));
				sb.append(line);
			}
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sb.toString();
	}
}