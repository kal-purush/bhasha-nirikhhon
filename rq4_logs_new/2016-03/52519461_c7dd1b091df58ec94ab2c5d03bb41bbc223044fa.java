package AutoJon.src;

import java.io.IOException;

import org.jsoup.Jsoup;

public class apply {

	public static void jobsite(String url) {
		System.out.println(url);
		
		try {
			org.jsoup.nodes.Document doc = Jsoup.connect(url).get();
			
			
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}