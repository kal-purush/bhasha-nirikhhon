package src;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.glassfish.hk2.utilities.reflection.Constants;

public class ShortenerApp {
	private void TinyURL() {} 
	 
    public static String getTinyUrl(String longUrl) throws IOException { 
        Map<String, String> params = new HashMap<String, String>(); 
        params.put("url", longUrl); 
        return NetUtil.doPost("http://tinyurl.com/api-create.php", params); 
    } 
    
	
    
    
}


	
 