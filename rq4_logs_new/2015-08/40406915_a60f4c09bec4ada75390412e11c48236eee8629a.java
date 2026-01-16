import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

public class Cryptsy extends Exchange {
	public Cryptsy(){//default constructor
		API_URL = "https://cryptsy.com/api/v2/";
		markets = getMarkets();
	}
	
	public void setAuthKeys(String key, String secret){	
		super.setAuthKeys(key, secret);
	}
	@Override
	public void preCall(){	
		while (nonce == lastNonce)
			nonce++;
		long elapsed = System.currentTimeMillis() - AuthLastRequest;
		if (elapsed < AuthRequestLimit) {
			try {
				Thread.currentThread().sleep(AuthRequestLimit - elapsed);
			} catch (InterruptedException e) {

			}
		}
		AuthLastRequest = System.currentTimeMillis();
	}
	@Override
	public String request(String urlstr){

		// handle precall logic
		preCall();

		// create connection
		URLConnection conn = null;
		StringBuffer response = new StringBuffer();
		try {
			URL url = new URL(urlstr);
			conn = url.openConnection();
			conn.setUseCaches(false);
			conn.setRequestProperty("User-Agent", USER_AGENT);

			// read response
			BufferedReader in = new BufferedReader(new InputStreamReader(
					conn.getInputStream()));
			String line = null;
			while ((line = in.readLine()) != null)
				response.append(line);
			in.close();
		} catch (MalformedURLException e) {
			System.out.println("Internal error." + e);
		} catch (IOException e) {
			System.out.println("Error connecting to Cryptsy." + e);
		}
		return response.toString();
	}
	@Override
	public String authRequest(String method, Map<String, String> args){
		if (!initialized)
			System.out.println("Cryptsy not initialized.");

		// prep the call
		preAuth();

		// add method and nonce to args
		if (args == null)
			args = new HashMap<String, String>();
		args.put("method", method);
		args.put("nonce", Long.toString(nonce));
		lastNonce = nonce;

		// create url form encoded post data
		String postData = "";
		for (Iterator<String> iter = args.keySet().iterator(); iter.hasNext();) {
			String arg = iter.next();
			if (postData.length() > 0)
				postData += "&";
			try {
				postData += arg + "=" + URLEncoder.encode(args.get(arg), java.nio.charset.StandardCharsets.UTF_8.toString());
			} catch (UnsupportedEncodingException e) {
				System.out.println("Charset error." + e);
			}
		}

		// create connection
		URLConnection conn = null;
		StringBuffer response = new StringBuffer();
		try {
			URL url = new URL(API_URL);
			conn = url.openConnection();
			conn.setUseCaches(false);
			conn.setDoOutput(true);
			conn.setRequestProperty("Key", key);
			conn.setRequestProperty("Sign",
					toHex(mac.doFinal(postData.getBytes("UTF-8"))));
			conn.setRequestProperty("Content-Type",
					"application/x-www-form-urlencoded");
			conn.setRequestProperty("User-Agent", USER_AGENT);

			// write post data
			OutputStreamWriter out = new OutputStreamWriter(
					conn.getOutputStream());
			out.write(postData);
			out.close();

			// read response
			BufferedReader in = new BufferedReader(new InputStreamReader(
					conn.getInputStream()));
			String line = null;
			while ((line = in.readLine()) != null)
				response.append(line);
			in.close();
				} catch (MalformedURLException e) {
					System.out.println("Internal error." + e);
				} catch (IOException e) {
					System.out.println("Error connecting to Cryptsy." + e);
				}
				return response.toString();
	}
	public void setRequestLimit(long reqLim){
		this.RequestLimit = reqLim;
	}
	public void setAuthRequestLimit(long reqLim){
		super.AuthRequestLimit = reqLim;
	}
	@Override
	public ArrayList<Exchange.Market> getMarkets(){
		JSONObject json = new JSONObject(request(API_URL+"markets/"));
		JSONArray data = json.getJSONArray("data");
		for(int index = 0; index < data.length(); index++){
			CurrencyPair c = new CurrencyPair(data.getJSONObject(index).getString("label"));
			Market m = new Market(c);
			m.MarketID = data.getJSONObject(index).getInt("id");
			m.coin_currency_id = data.getJSONObject(index).getInt("coin_currency_id");
			m.market_currency_id = data.getJSONObject(index).getInt("market_currency_id");
			m.maintenance_mode = data.getJSONObject(index).getBoolean("market_currency_id");//may break
			m.verifiedonly = data.getJSONObject(index).getBoolean("verifiedonly");
			
			JSONObject JSON24hr = data.getJSONObject(index).getJSONObject("24hr");
			JSONObject JSONLastTrade = data.getJSONObject(index).getJSONObject("last_trade");
			m._24HR.volume = JSON24hr.getDouble("volume");
			m._24HR.volume_btc = JSON24hr.getDouble("volume_btc");
			m._24HR.price_high = JSON24hr.getDouble("price_high");
			m._24HR.price_low = JSON24hr.getDouble("price_low");
			m.lastTrade.price = JSONLastTrade.getDouble("price");
			m.lastTrade.date = Date.valueOf(JSONLastTrade.getString("date"));
			m.lastTrade.timestamp = JSONLastTrade.getLong("timestamp");
			JSONObject feesJSON = new JSONObject();
			markets.add(m);
		}
		return markets;
	}
	@Override
	public List<String> getCurrencies(){
		//returns list of currencies (String)
		return null;
	}
	@Override
	public double getFees(CurrencyPair market){
		//returns double
		for(int index = 0; index < markets.size(); index++){
			if(markets.get(index).currencyPair.toString().equals(market.toString())){
				return markets.get(index).getFee();
			}
		}
		return 0.0;//something went wrong
	}
	@Override
	public void getOrderBook(String market){
		//returns orderBook
	}
	public class Market extends Exchange.Market{
		public int coin_currency_id;
		public int market_currency_id;
		public boolean maintenance_mode;
		public boolean verifiedonly;
		public double fee;
		public _24hr _24HR = new _24hr();
		public last_trade lastTrade = new last_trade();
		public class _24hr{
			double volume;
			double volume_btc;
			double price_high;
			double price_low;
		}
		public class last_trade{
			double price;
			java.util.Date date;
			long timestamp;
		}
		/*Constructors*/
		public Market(String currency1, String currency2){
			super(currency1, currency2);
		}
		public Market(String currencyPair){
			super(currencyPair);
		}
		public Market(CurrencyPair currencyPair){
			super(currencyPair);
		}
		public Market(int marketID){
			super(marketID);
		}
		/*Constructors*/
		/*Methods*/
		@Override
		public double getFee(){
			return 0.0;
		}
		@Override
		public double getVolume(){
			return 0.0;
		}
		@Override
		public double getCurrencyVolume(){
			return 0.0;
		}
		/*Methods*/
		
	}
	public class OrderBook extends Exchange.OrderBook{
		public Market market;

		public OrderBook(Exchange.Market market) {
			super(market);
			// TODO Auto-generated constructor stub
		}
		@Override
		public String getOrderBook(){
			return null;
		}
		@Override
		public String getBuyOrders(){
			return null;
		}
		@Override
		public String getAskOrders(){
			return null;
		}
	}
}