package plusplus;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import plusplus.client.Client;

public class Application {
	
	public static void main(String[] args) throws IOException, InterruptedException {
		
		ExecutorService service = Executors.newCachedThreadPool();
		
		//Custom server for examination purpose
//		Server server = new Server();
//		service.submit(server);
//		
//		Thread.sleep(1000);
		
//		Client client = new Client("localhost", 8888, 20);
		Client client = new Client("matilda.plusplus.rs", 4000, 20);
		service.submit(client);
		
		service.shutdown();
		
	}

}