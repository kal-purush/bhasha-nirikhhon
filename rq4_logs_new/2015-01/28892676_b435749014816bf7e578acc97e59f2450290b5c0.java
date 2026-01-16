package net.lisia21.wapl;

import java.net.URL;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import android.annotation.SuppressLint;
import android.app.Service;
import android.content.Intent;
import android.os.AsyncTask;
import android.os.IBinder;
import android.util.Log;
import android.widget.Toast;

public class UpdateService extends Service {
	private static final String TAG = "UpdateService";

	@Override
	public IBinder onBind(Intent intent) {
		return null;
	}

	@Override
	public void onCreate() {
		super.onCreate();
	}

	@Override
	public void onDestroy() {
		super.onDestroy();
	}

	@Override
	public int onStartCommand(Intent intent, int flags, int startId) {
		String xmlUrl = "http://203.247.166.59/01087060915/data.xml";

		GetXMLTask task = new GetXMLTask(this);
		task.execute(xmlUrl);
		return super.onStartCommand(intent, flags, startId);
	}

	@SuppressLint("NewApi")
	private class GetXMLTask extends AsyncTask<String, Void, Document> {
		private UpdateService context;

		public GetXMLTask(UpdateService updateService) {
			this.context = updateService;
		}

		@Override
		protected Document doInBackground(String... urls) {
			URL url;
			Document doc = null;

			try {
				url = new URL(urls[0]);

				DocumentBuilderFactory dbf = DocumentBuilderFactory
						.newInstance();
				DocumentBuilder db = dbf.newDocumentBuilder();

				doc = db.parse(new InputSource(url.openStream()));
				doc.getDocumentElement().normalize();

			} catch (Exception e) {
				Log.w("Connection", "ConnectionError");
			}

			return doc;
		}

		@Override
		protected void onPostExecute(Document doc) {

			if (doc.getElementsByTagName("user") != null) {

				if (doc.getElementsByTagName("raspberry").getLength() == 0) {
					onDestroy();
				} else {

					NodeList nodeList = doc.getElementsByTagName("raspberry");

					for (int i = 0; i < nodeList.getLength(); i++) {

						Node node = nodeList.item(i);
						Element fstElmnt = (Element) node;

						NodeList serialList = fstElmnt
								.getElementsByTagName("serial");

						Element serialElement = (Element) serialList.item(0);
						serialList = serialElement.getChildNodes();

						NodeList tempList = fstElmnt
								.getElementsByTagName("temperature");

						Element tempElement = (Element) tempList.item(0);
						tempList = tempElement.getChildNodes();

						NodeList humidList = fstElmnt
								.getElementsByTagName("humidity");

						Element humidElement = (Element) humidList.item(0);
						humidList = humidElement.getChildNodes();

						NodeList moistureList = fstElmnt
								.getElementsByTagName("moisture");

						Element moistureElement = (Element) moistureList
								.item(0);
						moistureList = moistureElement.getChildNodes();

						if (Integer.parseInt(((Node) moistureList.item(0))
								.getNodeValue()) < 300) {
							Toast.makeText(getApplicationContext(),
									serialList.item(0).getNodeValue()+ " needs Water", Toast.LENGTH_SHORT).show();
						}
					}
				}
			}

		}
	}
}