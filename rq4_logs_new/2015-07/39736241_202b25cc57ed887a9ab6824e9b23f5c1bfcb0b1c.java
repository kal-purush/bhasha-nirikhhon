/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.rproject;

//import java.util.logging.Logger;

import com.amazonaws.auth.BasicAWSCredentials;
import edu.cmu.sphinx.api.Configuration;
import edu.cmu.sphinx.api.SpeechResult;
import edu.cmu.sphinx.api.StreamSpeechRecognizer;
import edu.cmu.sphinx.demo.transcriber.TranscriberDemo;
import edu.cmu.sphinx.result.WordResult;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import java.io.BufferedReader;
//import com.amazonaws.util.IOUtils;
import org.apache.commons.io.IOUtils;

//import org.apache.commons.compress.utils.IOUtils;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.STSSessionCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.handlers.RequestHandler;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author paulamontojo
 */
public class runnableClass {
    
    static AmazonDynamoDBClient dynamoDBClient;
     public static String TABLENAME = "finalTables";
    static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    static String productCatalogTableName = "tableExample";
    
    private static final String bucketName     = "pdsphinxbucket";
    private static FileReader fr;
    private static BufferedReader br;
    private static StringBuilder sbu;
    
    public static void main(String args[]) throws Exception {
          try {
    System.out.println("Loading driver...");
    Class.forName("com.mysql.jdbc.Driver");
    System.out.println("Driver loaded!");
  } catch (ClassNotFoundException e) {
    throw new RuntimeException("Cannot find the driver in the classpath!", e);
  }
         use();
          
    }
    public static void use() throws IOException{
       AWSCredentials awsCreds = new PropertiesCredentials(new File("/Users/paulamontojo/Desktop/AwsCredentials.properties"));

        AmazonSQS sqs = new AmazonSQSClient(awsCreds);
        
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        sqs.setRegion(usWest2);
        String myQueueUrl = "https://sqs.us-west-2.amazonaws.com/711690152696/MyQueue";
        
       System.out.println("Receiving messages from MyQueue.\n");
       
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            while(messages.isEmpty()){
          
               messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            }


         String messageRecieptHandle = messages.get(0).getReceiptHandle();

            String a = messages.get(0).getBody();

            sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageRecieptHandle));
            
            //aqui opero y cuando acabe llamo para operar el siguiente.
            
     
       
              String n = "";          
  String dbName = "mydb";
  String userName = "pmontojo";
  String password = "pmontojo";
  String hostname = "mydb.cued7orr1q2t.us-west-2.rds.amazonaws.com";
  String port = "3306";
  String jdbcUrl = "jdbc:mysql://" + hostname + ":" +
    port + "/" + dbName + "?user=" + userName + "&password=" + password;
    Connection conn = null;
  Statement setupStatement = null;
  Statement readStatement = null;
  ResultSet resultSet = null;
  String results = "";
  int numresults = 0;
  String statement = null;

  try {
   
    conn = DriverManager.getConnection(jdbcUrl);

    setupStatement = conn.createStatement();

   String insertUrl = "select video_name from metadata where id = " + a + ";";
   String checkUrl = "select url from metadata where id = " + a + ";";

   ResultSet rs =  setupStatement.executeQuery(insertUrl);

    rs.next();

     System.out.println("este es el resultdo " + rs.getString(1));
     
     String names = rs.getString(1);
        ResultSet ch = setupStatement.executeQuery(checkUrl);
           ch.next();
           System.out.println("este es la url" + ch.getString(1));
           String urli = ch.getString(1);
     
     while(urli == null){
         ResultSet sh = setupStatement.executeQuery(checkUrl);
           sh.next();
           System.out.println("este es la url" + sh.getString(1));
           urli = sh.getString(1);
         
     }
     setupStatement.close();
     AmazonS3 s3Client = new AmazonS3Client(awsCreds); 
  
     S3Object object = s3Client.getObject(
     new GetObjectRequest(bucketName, names));

    IOUtils.copy(object.getObjectContent(), new FileOutputStream(new File("/Users/paulamontojo/Desktop/download.avi")));
     

    putOutput();
    write();
    putInDb(sbu.toString(),a);
    
  } catch (SQLException ex) {
    // handle any errors
    System.out.println("SQLException: " + ex.getMessage());
    System.out.println("SQLState: " + ex.getSQLState());
    System.out.println("VendorError: " + ex.getErrorCode());
  } finally {
    System.out.println("Closing the connection.");
    if (conn != null) try { conn.close(); } catch (SQLException ignore) {}
  }
            

           
            use();
            

    }
    
    public static void putOutput() throws IOException{
        Runtime r = Runtime.getRuntime();
  
           StringBuilder sbuilder = new StringBuilder("/usr/local/bin/ffmpeg -i /Users/paulamontojo/Desktop/download.avi -acodec pcm_s16le -ac 1 -ar 16000 /Users/paulamontojo/Desktop/duprueita.wav");
                 long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            try {Thread.sleep(1000 * 20);} catch (Exception e) {}}
      
           StringBuilder sbuilder2 = new StringBuilder("mv -f /Users/paulamontojo/Desktop/duprueita.wav /Users/paulamontojo/Downloads/rproject/src/main/resources/edu/cmu/sphinx/demo/aligner/outpone.wav");
          
            Process p = r.exec(sbuilder.toString());
            Process p2 = r.exec(sbuilder2.toString());
    }
    
    public static void write(){
        try {
           
            
            System.out.println("Loading models...");
            
            
            Configuration configuration = new Configuration();
            
            // Load model from the jar
            
            
            configuration
                    .setAcousticModelPath("resource:/edu/cmu/sphinx/models/en-us/en-us");
            
            //diccionario que usamos
            
            configuration
                    .setDictionaryPath("resource:/edu/cmu/sphinx/models/en-us/cmudict-en-us.dict");
            
            //languagemodel 
            
            configuration
                    .setLanguageModelPath("resource:/edu/cmu/sphinx/models/en-us/en-us.lm.dmp");
            

//        //creo un reconozcedor
            StreamSpeechRecognizer recognizer = new StreamSpeechRecognizer(
                    configuration);
            System.out.println("wuhu");
            InputStream stream = TranscriberDemo.class
                    .getResourceAsStream("/edu/cmu/sphinx/demo/aligner/outpone.wav");
           
            
            System.out.println("holotuhu");
            
//            
            stream.skip(44);
            
            System.out.println("wuhuluhutuhu");
            
            // Simple recognition with generic model
            recognizer.startRecognition(stream);
            SpeechResult result;

            String outputFilePath = "/Users/paulamontojo/Downloads/rproject/src/main/resources/edu/cmu/sphinx/demo/aligner/prueba.txt";
            
            PrintWriter pw = null;
            try {
                pw = new PrintWriter(new BufferedWriter(new FileWriter(outputFilePath,
                        false)));
                // Loop until last utterance in the audio file has been decoded, in which case the recognizer will return null.
               
                
                while ((result = recognizer.getResult()) != null) {
                    
                    //get hypothesis 
                    System.out.format("Hypothesis: %s\n", result.getHypothesis());
                    
                    
                    for (WordResult res : result.getWords()) {
                        
                        if(res.toString().contains("<sil>")){
                            System.out.println(" ");
                            pw.println(" ");
                        }
                        else{
                            System.out.print(res);
                            pw.print(res);
                        }
                    }
                    
                    System.out.println("Best hypothesis:");
                    for (String s : result.getNbest(1)){
                        
                        System.out.println(s);
                        
                        System.out.println("holi");
                    }
                    
                    
                }
            } catch (IOException ex) {
               
            } finally {
                if (pw != null) {
                    pw.close();
                    pw.flush();
                   
                    
                } }
            
          
            recognizer.stopRecognition();
          try {
         
        File archivo = new File ("/Users/paulamontojo/Downloads/rproject/src/main/resources/edu/cmu/sphinx/demo/aligner/prueba.txt");
         fr = new FileReader (archivo);
        br = new BufferedReader(fr);
        sbu = new StringBuilder();
 
         // Lectura del fichero
         String linea;
         while((linea=br.readLine())!=null){
            System.out.println(linea);
         sbu.append(linea);
         sbu.append("\n");
         }
         
         
      }
          
          
      catch(Exception e){
         e.printStackTrace();
      }finally{
         
         try{                    
            if( null != fr ){   
               fr.close();     
            }                  
         }catch (Exception e2){ 
            e2.printStackTrace();
         }
      }
          
          System.out.println(sbu.toString());
           
        } catch (IOException ex) {           Logger.getLogger(runnableClass.class.getName()).log(Level.SEVERE, null, ex);
           
        }
    }
    
    public static void putInDb(String s, String id) throws IOException{
        AWSCredentials credentials = new PropertiesCredentials(new File("/Users/paulamontojo/Desktop/AwsCredentials.properties"));

        dynamoDBClient = new AmazonDynamoDBClient(new STSSessionCredentialsProvider(credentials));
        dynamoDBClient.setRegion(Region.getRegion(Regions.US_WEST_2));
            createTable();

            // Describe our new table
            describeTable();

            // Add some items
            putItem(newItem(id,"url",s));

    }
    
    private static void getItem(String keyVal) {
        
        Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("ID", new AttributeValue(keyVal));
        
        GetItemRequest getItemRequest = new GetItemRequest()
            .withTableName(TABLENAME)
            .withKey(key);
        
        GetItemResult item = dynamoDBClient.getItem(getItemRequest);
        
        System.out.println("Get Result: " + item);
    }

    
    private static void createTable() {
        List<AttributeDefinition> attributeDefinitions= new ArrayList<AttributeDefinition>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("ID").withAttributeType("S"));
                
        List<KeySchemaElement> ks = new ArrayList<KeySchemaElement>();
        ks.add(new KeySchemaElement().withAttributeName("ID").withKeyType(KeyType.HASH));
          
        ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
            .withReadCapacityUnits(10L)
            .withWriteCapacityUnits(10L);
                
        CreateTableRequest request = new CreateTableRequest()
            .withTableName(TABLENAME)
            .withAttributeDefinitions(attributeDefinitions)
            .withKeySchema(ks)
            .withProvisionedThroughput(provisionedThroughput);
            
        try {
            
            CreateTableResult createdTableDescription = dynamoDBClient.createTable(request);
      
            System.out.println("Created table:" + createdTableDescription);
            // Wait for it to become active
            waitForTableToBecomeAvailable(TABLENAME);

        } catch (ResourceInUseException e) {

            //logger.warn(e);
        }
    }
    
    private static void waitForTableToBecomeAvailable(String tableName) {


        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            try {Thread.sleep(1000 * 20);} catch (Exception e) {}
            try {
                DescribeTableRequest request = new DescribeTableRequest().withTableName(tableName);
                TableDescription tableDescription = dynamoDBClient.describeTable(request).getTable();
                String tableStatus = tableDescription.getTableStatus();
              
                System.out.println("tableStatus" + tableStatus);
                if (tableStatus.equals(TableStatus.ACTIVE.toString())) return;
            } catch (AmazonServiceException ase) {
                if (ase.getErrorCode().equalsIgnoreCase("ResourceNotFoundException") == false) throw ase;
            }
        }

        throw new RuntimeException("Table " + tableName + " never went active");
    }
      private static void describeTable() {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(TABLENAME);
        TableDescription tableDescription = dynamoDBClient.describeTable(describeTableRequest).getTable();
//        logger.info("Table Description: " + tableDescription);
        System.out.println("desc:v"+ tableDescription);
    }
        private static void putItem(Map<String, AttributeValue> item) {
        
        try {

            PutItemRequest putItemRequest = new PutItemRequest(TABLENAME, item);
            PutItemResult putItemResult = dynamoDBClient.putItem(putItemRequest);
      System.out.println("putresult " + putItemResult);
        } catch (Exception e) {
            // TODO: handle exception
        }
    }
        
        private static Map<String, AttributeValue> newItem(String ID, String url, String text) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("ID", new AttributeValue(ID));
        item.put("url", new AttributeValue(url));
        item.put("text", new AttributeValue(text));
  

        return item;
    }
    }
