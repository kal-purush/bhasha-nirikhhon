import com.jaunt.Element;
import com.jaunt.Elements;
import com.jaunt.UserAgent;

public class Main {

	public static void main(String[] args) throws Exception {
		scrap("http://www.toonnetworkindia.co.in/2015/09/pokemon-advanced-hindi-episodes-hungama.html");
		
	}
	public static int scrap(String u) throws Exception {
		UserAgent useragent = new UserAgent();
		useragent.visit(u);
		Elements links = useragent.doc.findEvery("<a>");
		
		for (Element link : links){
			System.out.println(link.getAt("href"));
			
		}
		return 0;
		
	}

}