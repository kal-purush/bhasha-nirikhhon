import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.util.Calendar;      
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

public class Client implements Runnable {

    private List<Message> history = new ArrayList<Message>();
    private MessageExchange messageExchange = new MessageExchange();
    private String host;
    private Integer port;
    private String  username;

    public Client(String host, Integer port) {
    	Random r = new Random();
        this.username = "User " + String.valueOf(r.nextInt(10000));
    	this.host = host;
        this.port = port;
    }
    
    static PrintWriter outClient;
    static FileWriter fileWriter;
    static Calendar calendar;
    static String s = "";
    static Date now;
    static DateFormat formatter;
    static String fileName = "clientlog.txt";

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: java ChatClient host port");
        } else {
        	myPrint("Connection to server...");	   
            String serverHost = args[0];
            Integer serverPort = Integer.parseInt(args[1]);
            Client client = new Client(serverHost, serverPort);
            new Thread(client).start();
            myPrint("Connected to server: " + serverHost + ":" + serverPort);
            client.listen();    
        }      
    }
	
    public static void myPrint(String S) {
    	System.out.println(S);
    	now = new Date();    
    	formatter = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
     	s = formatter.format(now);
     	try {
       		FileWriter sw = new FileWriter(fileName, true); 
       		sw.write(s + " " + S + "\n");   
	        sw.close();
        } catch(Exception e){ 
	        System.out.print(e.getMessage());  
        }       
    }

    private HttpURLConnection getHttpURLConnection() throws IOException {
        URL url = new URL("http://" + host + ":" + port + "/chat?token=" + messageExchange.getToken(history.size()));
        return (HttpURLConnection) url.openConnection();
    }

    public List<Message> getMessages() {
        List<Message> list = new ArrayList<Message>();
        HttpURLConnection connection = null;
        try {
            connection = getHttpURLConnection();
            connection.connect();
            String response = messageExchange.inputStreamToString(connection.getInputStream());
            JSONObject jsonObject = messageExchange.getJSONObject(response);
            JSONArray jsonArray = (JSONArray)jsonObject.get("messages");
            for (Object o : jsonArray) {
            	String s = o.toString();
                JSONObject obj = messageExchange.getJSONObject(s);
                Message msg = new Message(obj);
                myPrint(msg.getMessage());
                list.add(msg);
            }
        } catch (IOException e) {
            System.err.println("ERROR: " + e.getMessage());
        } catch (ParseException e) {
            System.err.println("ERROR: " + e.getMessage());
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
        return list;
    }

    public void sendMessage(String message) {
        HttpURLConnection connection = null;
        try {
            connection = getHttpURLConnection();
            connection.setDoOutput(true);
            connection.setRequestMethod("POST");
            DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
            byte[] bytes = messageExchange.getClientSendMessageRequest(new Message(username, message)).getBytes();
            wr.write(bytes, 0, bytes.length);
            wr.flush();
            wr.close();
            connection.getInputStream();
        } catch (IOException e) {
            System.err.println("ERROR: " + e.getMessage());
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    public void listen() {
        while (true) {
        	List<Message> list = getMessages();
            if (list.size() > 0) {
                history.addAll(list);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.err.println("ERROR: " + e.getMessage());
            }
        }
    }

    @Override
    public void run() {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            String message = scanner.nextLine();
            sendMessage(message);
        }
    }
}