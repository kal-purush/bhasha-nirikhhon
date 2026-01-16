package example.webcrawler;

import java.io.IOException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * This class is responsible to parse give web URL using jsoup libraries
 * 
 * @author Admin
 *
 */
public class Extracter {

	/**
	 * It takes URL string and print all links, images, imports and resources
	 * that are available in the page
	 * 
	 * @param URL
	 *            <code>String</code>
	 * @throws IOException
	 */
	public static void printWeb(String URL) throws IOException {

		print("Fetching %s...", URL);

		Document doc = Jsoup.connect(URL).get();
		Elements links = doc.select("a[href]");
		Elements media = doc.select("[src]");
		Elements imports = doc.select("link[href]");

		print("\nMedia: (%d)", media.size());
		for (Element src : media) {
			if (src.tagName().equals("img"))
				print(" * %s: <%s> %sx%s (%s)", src.tagName(), src.attr("abs:src"), src.attr("width"),
						src.attr("height"), trim(src.attr("alt"), 20));
			else
				print(" * %s: <%s>", src.tagName(), src.attr("abs:src"));
		}

		print("\nImports: (%d)", imports.size());
		for (Element link : imports) {
			print(" * %s <%s> (%s)", link.tagName(), link.attr("abs:href"), link.attr("rel"));
		}

		print("\nLinks: (%d)", links.size());
		for (Element link : links) {
			print(" * a: <%s>  (%s)", link.attr("abs:href"), trim(link.text(), 35));
		}
	}
	
/**
 * It prints the given string with format
 * @param msg
 * @param args
 */
	private static void print(String msg, Object... args) {
		System.out.println(String.format(msg, args));
	}
/**
 * 
 * @param str
 * @param width
 * @return
 */
	private static String trim(String str, int width) {
		if (str.length() > width)
			return str.substring(0, width - 1) + ".";
		else
			return str;
	}

}