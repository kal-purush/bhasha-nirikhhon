package src;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.glassfish.hk2.utilities.reflection.Constants;
import com.rosaloves.bitlyj.Url;
import static com.rosaloves.bitlyj.Bitly.*;

public class ShortenerApp {
	private void urlApp() {
		
		Url url = as("o_1ki0u1sud9", "R_f4121ab5e7284f9fabfbef18eb7db250").call(shorten("http://www.dnb.com"));
		String ul = url.getShortUrl();
		System.out.println("art bitfly:"+ url);
	}

}