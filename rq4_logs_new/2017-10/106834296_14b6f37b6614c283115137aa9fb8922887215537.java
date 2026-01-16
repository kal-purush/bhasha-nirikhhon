package RestPages;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;

public class DeleteServicePage {

	public void deleteMethod() throws ClientProtocolException, IOException
	{
		HttpClient httpClient = HttpClientBuilder.create().build();

		HttpDelete deleterequest = new HttpDelete(
				"http://www.thomas-bayer.com/sqlrest/PRODUCT/55");

		ArrayList<BasicNameValuePair> nvps = new ArrayList<BasicNameValuePair>();
		BasicNameValuePair bn;

		bn = new BasicNameValuePair("content-type", "application/xml");
		nvps.add(bn);
		bn = new BasicNameValuePair("Server", "Apache-Coyote/1.1");
		nvps.add(bn);	    


		for (BasicNameValuePair h : nvps) {
			deleterequest.addHeader(h.getName(), h.getValue());
		}
		HttpResponse response = httpClient.execute(deleterequest);

		if (response.getStatusLine().getStatusCode() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatusLine().getStatusCode());
		}
		else
			System.out.println("Record has been deleted successfully");
	}
}


