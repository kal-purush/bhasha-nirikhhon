import java.net.URLConnection;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.charset.Charset;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

public class SendHouses {
	public static void main(String[] args) throws IOException {
		String json = readFile("data.json");

		String landlord_id = args[1];

		System.out.println(landlord_id);

        // String response = post(json, landlord_id, auth);
        
        // System.out.println(response);
    }

    public static String post(String json, String landlord_id, String auth) throws IOException {
    	HttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost("https://localhost/sturents.com/api/houses");
        
        HttpEntity entity = new ByteArrayEntity(json.getBytes("UTF-8"));
        
        post.setEntity(entity);

        HttpResponse response = client.execute(post);
        String result = EntityUtils.toString(response.getEntity());

        return result;
    }

	static String readFile(String path) throws IOException  {
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		
		return new String(encoded, "UTF-8");
	}
}