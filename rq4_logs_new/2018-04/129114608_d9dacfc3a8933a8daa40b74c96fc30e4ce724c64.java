package src;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class URLShortener {

	public static void main(String[] args) throws Exception {


		List<String> urls= ReadFromFile.urls();
		for (int i = 0; i < urls.size(); i++) {
			
			System.out.println(urls.get(i));
		}
		
		
	}
	
	public String shortenURL(String longURL) {
		String shortURL = "";
		if (validateURL(longURL)) {
			longURL = sanitizeURL(longURL);
			if (valueMap.containsKey(longURL)) {
				shortURL = domain + "/" + valueMap.get(longURL);
			} else {
				shortURL = domain + "/" + getKey(longURL);
			}
		}
		// add http part
		return shortURL;
	}
	

}
