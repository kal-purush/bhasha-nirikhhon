package echo;

import java.net.Socket;
import java.util.HashMap;

class Cache extends HashMap<String, String> {
	
	  // String search(String request) { ... }
	Cache(){
		super();
	}
	public synchronized String search(String request) {
		String output = "";
		if(this.containsKey(request)) {
			output = this.get(request);
		   }
		   else {
			   return null;
		 }
		return output;
	}
	public synchronized void update(String request, String response) {
		this.put(request, response);
	}
	   //update(String request, String response) { ... }
}

public class CacheHandler extends ProxyHandler {
	   private static Cache cache = new Cache();
	   public CacheHandler(Socket s) { super (s);}
	   public CacheHandler() {super();}
	   // add constructor that call super
	   
	   protected String response(String request) throws Exception {
		   String output = "";
	      // 1. cache.search(request)
		   String find = cache.search(request);
	      if(find!= null) {
	    	  output = find;
	    	  output += "\t (Void in MathHandler)";
	      }
	      else {
	    	  output = super.response(request);
	    	  cache.update(request, output);
	      }
		   return output;
	   }
}