package com.teradata.kylofeedaccelerator;


import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.core.io.FileSystemResource;

import java.util.Base64;
import org.springframework.web.client.RestTemplate;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


@SpringBootApplication 

public class KyloFeedPromoter 
{
	public static final Logger log = LoggerFactory.getLogger(KyloFeedPromoter.class);
	
	//load from properties file : KYLO rest urls for get-feeds,export-feed,import-feed
	public static  String SOURCE_GET_FEEDS_URL;
	public static String SOURCE_EXPORT_FEED_URL;
	public static  String TARGET_IMPORT_FEED_URL;

	
	//load from properties file : path to which feed zip from source gets exported to, and from which it gets imported to target
	public static  String FEED_DOWNLOAD_PATH;	

	
	//load from properties file : Credentials to the rest endpoint
    
	public static  String SOURCE_USER_NAME;
    public static  String SOURCE_PASSWORD;
    public static  String TARGET_USER_NAME;
    public static  String TARGET_PASSWORD;
    public static String[] FEEDS; 
    public static String PromoteAllFeeds;

    //to hold total no of feeds
    public static long sourceRecordsTotal;
    
    //to hold inputed systemFeedName and feedId
	public static HashMap<String, String> feedNameIdValues = new HashMap<String, String>();
	
	//to hold feednumbers to be promoted
	public static HashMap<String, String> feedNameIdToPromote = new HashMap<String, String>();	
	public static InputStream inputStream;

		
	public static void main(String[] args) throws ParseException, IOException 
	{
		
		//log.info("ERROR");
		SpringApplication.run(KyloFeedPromoter.class, args);
		System.out.println("***************** Welcome to Kylo Feed Promotion Accelerator *****************");

		
		//LOADING PROPERTY FILE values
		try {
			Properties prop = new Properties();
			String propFileName = "config.properties";
			inputStream = KyloFeedPromoter.class.getClassLoader().getResourceAsStream(propFileName); 
			if (inputStream != null) 
			{
				prop.load(inputStream);
			} else 
			{
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}
			
			SOURCE_GET_FEEDS_URL = prop.getProperty("SOURCE_GET_FEEDS_URL");
			SOURCE_EXPORT_FEED_URL = prop.getProperty("SOURCE_EXPORT_FEED_URL");
			TARGET_IMPORT_FEED_URL = prop.getProperty("TARGET_IMPORT_FEED_URL");
			FEED_DOWNLOAD_PATH = prop.getProperty("FEED_DOWNLOAD_PATH");
			FEEDS = prop.getProperty("FEEDS").split(",");
			PromoteAllFeeds=prop.getProperty("PromoteAllFeeds");
			SOURCE_USER_NAME = prop.getProperty("SOURCE_USER_NAME");
			SOURCE_PASSWORD = prop.getProperty("SOURCE_PASSWORD");
			TARGET_USER_NAME = prop.getProperty("TARGET_USER_NAME");
			TARGET_PASSWORD = prop.getProperty("TARGET_PASSWORD");	

		} 
		catch (Exception e) 
		{
			System.out.println("Exception: " + e.getMessage());
			System.exit(0);
		} 
				
		inputStream.close();

		//Creating basic authorization headers & Request to return JSON format
		HttpHeaders headers = new HttpHeaders();
		String auth = SOURCE_USER_NAME + ":" + SOURCE_PASSWORD;
		byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(Charset.forName("US-ASCII")));
		String authHeader = "Basic " + new String(encodedAuth);
		headers.set("Authorization", authHeader);
		headers.setAccept(Arrays.asList(new MediaType[] { MediaType.APPLICATION_JSON }));
		headers.setContentType(MediaType.APPLICATION_JSON);
 
		HttpEntity<String> entity = new HttpEntity<String>(headers);
		RestTemplate restTemplate = new RestTemplate();
		

		//method call - get feeds
		String responseString = methodToGetFeeds(restTemplate,entity);
		
		//method call - parse json & store into map objects
		if(responseString!=null)
		{
			methodToParse(responseString,sourceRecordsTotal,feedNameIdValues);

		}
		else
		{
			System.exit(0);
		}
				
		//Method cal - Inputing feednumber from user 
		validateFeedsAgainstSource(PromoteAllFeeds,FEEDS,feedNameIdValues,feedNameIdToPromote);		
		
		
		System.out.println("********************* Exporting user specified feed(s) from source : *************************");
		System.out.println();
		RestTemplate exportRestTemplate = new RestTemplate();
		//Sending in credentials as headers
		HttpHeaders exportheaders = new HttpHeaders();
		//for basic authorization
		exportheaders.set("Authorization", authHeader);
		//setting the content type
		exportheaders.setAccept(Arrays.asList(MediaType.APPLICATION_OCTET_STREAM,MediaType.ALL));
		exportheaders.setContentType(MediaType.APPLICATION_OCTET_STREAM);
		HttpEntity<String> exportEntity = new HttpEntity<String>(exportheaders);
		
		
		File file = new File(FEED_DOWNLOAD_PATH);
		file.mkdirs();
		
		exportFeeds(exportRestTemplate,exportEntity,feedNameIdToPromote);
		System.out.println("***************** All specified feeds exported to temp directory *****************");
		System.out.println();
		
		System.out.println("***************** Importing user specified feeds to target *****************");
		RestTemplate importRestTemplate = new RestTemplate();
		HttpHeaders importheaders = new HttpHeaders();
		
		//Creating basic authorization headers
		String authTarget = TARGET_USER_NAME + ":" + TARGET_PASSWORD;
		byte[] encodedAuthTarget = Base64.getEncoder().encode(authTarget.getBytes(Charset.forName("US-ASCII")));
		String authHeaderTarget = "Basic " + new String(encodedAuthTarget);
		importheaders.set("Authorization", authHeaderTarget);
		
		importheaders.setAccept(Arrays.asList(MediaType.APPLICATION_JSON,MediaType.ALL));
		importheaders.setContentType(MediaType.MULTIPART_FORM_DATA);
		    
		importFeeds(importRestTemplate,importheaders,feedNameIdToPromote);
		System.out.println("***************** All specified feeds imported to target *****************");
		System.out.println();
		//Temp directory delete
		System.out.println("Deleting temp directory "+FEED_DOWNLOAD_PATH);
		FileUtils.deleteDirectory(file);
		System.exit(0);
	}
	

	//endOfMain

	private static void importFeeds(RestTemplate importRestTemplate, HttpHeaders importheaders,
			HashMap<String, String> feedNameIdToPromote) {
		// TODO Auto-generated method stub
		
		Iterator<Entry<String, String>> entriesSet3 = feedNameIdToPromote.entrySet().iterator();
		while(entriesSet3.hasNext())
		{
			Entry<String, String> entry = entriesSet3.next();
			System.out.println("***************** "+entry.getKey()+" import started *****************");
			MultiValueMap<String, Object> bodyMap = new LinkedMultiValueMap<String, Object>();
			bodyMap.add("categorySystemName", null);
			bodyMap.add("templateProperties", null);
			bodyMap.add("overwrite", true);
			bodyMap.add("overwriteFeedTemplate", true);
			bodyMap.add("importConnectingReusableFlow", "YES");
			bodyMap.add("feedProperties", null);
	    	bodyMap.add("file", new FileSystemResource(new File(FEED_DOWNLOAD_PATH.concat("\\").concat(entry.getKey()).concat(".feed.zip"))));
	    	
	        HttpEntity<MultiValueMap<String, Object>> importEntity = new HttpEntity<MultiValueMap<String,Object>>(bodyMap, importheaders);
	        ResponseEntity<String> importResponse = importRestTemplate.exchange(TARGET_IMPORT_FEED_URL, HttpMethod.POST,
	        		importEntity, String.class);
	        System.out.println("HTTP Status code of response for import url : "+importResponse.getStatusCode());
	        String resp = importResponse.getBody();
			JSONParser parseimp = new JSONParser();
			Boolean Validimp = false;
			Boolean Successimp = false;
			//Type caste the parsed json data to json object
			JSONObject jobjimp = null;
			try {
				jobjimp = (JSONObject) parseimp.parse(resp);
				Validimp = (Boolean) jobjimp.get("valid");
				//System.out.println(Validimp);
				Successimp = (Boolean) jobjimp.get("success");
				//System.out.println(Successimp);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        if(Validimp==true && Successimp==true)
	        {
	    		System.out.println("***************** "+entry.getKey()+" import successful*****************");
	        }
	        else
	        {
	        	System.out.println("***************** "+entry.getKey()+" import was not successful*****************");
	        }
	        System.out.println();
		}
		
	}
	
	private static void exportFeeds(RestTemplate exportRestTemplate, HttpEntity<String> exportEntity, HashMap<String, String> feedNameIdToPromote) {
		// TODO Auto-generated method stub
		Iterator<Entry<String, String>> entriesSet2 = feedNameIdToPromote.entrySet().iterator();
		while(entriesSet2.hasNext())
		{
			
			Entry<String, String> entry = entriesSet2.next();
			System.out.println("***************** "+entry.getKey()+" export started *****************");
			ResponseEntity<byte[]> exportResponse = exportRestTemplate.
					exchange(SOURCE_EXPORT_FEED_URL.concat("/").concat(entry.getValue()), HttpMethod.GET,
					exportEntity, byte[].class);
			
			System.out.println("HTTP Status code of response for export url :");
			System.out.println(exportResponse.getStatusCode());
			FileOutputStream fileOutputStream = null;
			try {
				fileOutputStream = new FileOutputStream(FEED_DOWNLOAD_PATH.concat("\\").concat(entry.getKey())
						.concat(".feed.zip"));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			System.out.println("Zip folder containing feed related files created at location : "+FEED_DOWNLOAD_PATH.concat("\\"));
			try {
				org.apache.commons.io.IOUtils.write(exportResponse.getBody(),fileOutputStream);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				fileOutputStream.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("***************** "+entry.getKey()+" export completed!");
			System.out.println();
		}
		
		
	}

	private static void validateFeedsAgainstSource(String PromoteAllFeeds,String[] FEEDS, 
			HashMap<String, String> feedNameIdValues, HashMap<String, String> feedNameIdToPromote) 
	{
		// TODO Auto-generated method stub
		System.out.println("***************** Validating user specified feeds against source *****************");	
		if(PromoteAllFeeds.equalsIgnoreCase("NO") && (!FEEDS[0].equalsIgnoreCase("ALL")))
		{
			for(int i=0;i<FEEDS.length;i++)
			{
				Iterator<Entry<String, String>> entriesSet1 = feedNameIdValues.entrySet().iterator();
				while(entriesSet1.hasNext())
				{
					Entry<String,String> entry = entriesSet1.next();
					if(FEEDS[i].equalsIgnoreCase(entry.getKey()))
					{
						System.out.println("Feed -> "+entry.getKey()+" exists in source.");
						feedNameIdToPromote.put(entry.getKey(),entry.getValue());
					}
				}
			}
			System.out.println("***************** feedNameId that will be promoted *****************");
			System.out.println(feedNameIdToPromote);
		}
		else if(PromoteAllFeeds.equalsIgnoreCase("YES") && (FEEDS[0].equalsIgnoreCase("ALL")))
		{
			feedNameIdToPromote.putAll(feedNameIdValues);
			System.out.println("***************** feedNameId that will be promoted *****************");
			System.out.println(feedNameIdToPromote);
		}
		else
		{
			System.out.println("Please check the config.properties file and set values correctly");
		}
	}
	
	private static void methodToParse(String responseString, long sourceRecordsTotal,
			HashMap<String, String> feedNameIdValues) {
		// TODO Auto-generated method stub
		//JSONParser reads the data from string object and break each data into key value pairs
				JSONParser parse = new JSONParser();
		    
				//Type caste the parsed json data to json object
				JSONObject jobj = null;
				try {
					jobj = (JSONObject) parse.parse(responseString);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    
				System.out.println("***************** Total no of existing feeds in source environment : *****************");
				System.out.println(jobj.get("recordsTotal"));
				sourceRecordsTotal = (long) jobj.get("recordsTotal");
				JSONArray jsonFeedsArray = (JSONArray) jobj.get("data");
				//iterating over "data" array of values
				for(int i=0; i<jsonFeedsArray.size();i++)
				{
				
					//Getting set of values under "data" header
					JSONObject jsonFeedObject = (JSONObject)jsonFeedsArray.get(i);
				
					//Storing the feednames & feedids to 2 map objects
					feedNameIdValues.put((String)jsonFeedObject.get("systemFeedName"), (String)jsonFeedObject.get("feedId"));
				}		
	}


	private static String methodToGetFeeds(RestTemplate restTemplate, HttpEntity<String> entity) 
	{
		//passing kylo "get feeds" API uri
		
		String responseString = null;
		ResponseEntity<String> response = restTemplate.exchange(SOURCE_GET_FEEDS_URL,
	    		HttpMethod.GET, entity, String.class);
		
		System.out.println("***************** Checking if the source kylo environment is up or not *****************");
		if(response.getStatusCode().is2xxSuccessful())
		{
			responseString = response.getBody().toString(); 
			System.out.println("Source Kylo environment is up");
		}else
		{
			responseString = null;
		}
		return responseString;
	}
	
}//endOfCLass
