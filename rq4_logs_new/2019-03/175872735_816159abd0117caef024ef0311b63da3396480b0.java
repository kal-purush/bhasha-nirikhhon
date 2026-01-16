// Java implementation for a client 
// Save file as Client.java 

import image.EncryptedPoint;
import image.ImageRepresentation;
import image.MyEncryptedImage;
import image.MyImage;
import image.Point;

import java.awt.Color;
import java.io.*; 
import java.math.BigInteger;
import java.net.*; 
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Scanner; 
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import javax.imageio.ImageIO;

import matrix.MultiplyMatrix;

import paillierPrototype.PaillierAlgorithm;
import utils.FileUtils;
import utils.JsonUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

// Client class 
public class Client {
	
	private static boolean trusted = false;
	
	public static MyImage myImage;
	private static PaillierAlgorithm paillier;
	public static String paillierJson;
	public static int[][] decryptedImage = null;
	public static ArrayList<String> imageArrayRepresentation = new ArrayList<String>();
	
	
	public static void generateImageRepresentation() {
	    
	   
	    myImage.iR = new ImageRepresentation(myImage.eI.width, myImage.eI.height);
	    //imageRGBMatrix = new int[image.getWidth()][image.getHeight()];
	    
	    for (int i = 0; i < myImage.eI.width; i++){
	    	for (int j = 0; j < myImage.eI.height; j++){
	    		//System.out.println(image.getRGB(i, j));
	    		Color c = new Color(myImage.imageRGBMatrix[i][j]);
	    		int red = c.getRed();
	    		int green = c.getGreen();
	    		int blue = c.getBlue();
	    		Point p = new Point(red, green, blue);
	    		myImage.iR.points[i][j] = p;
	    		//imageRGBMatrix[i][j] = image.getRGB(i, j);
	    	}
	    }
	}
	
	public static void generateEncryptedImage(PaillierAlgorithm paillier) {
		
		BigInteger helper = BigInteger.ZERO;
		
		myImage.eI = new MyEncryptedImage(myImage.eI.width, myImage.eI.height);
		System.out.println("1");
		for (int i = 0; i < myImage.eI.width; i++){
	    	for (int j = 0; j < myImage.eI.height; j++){
	    		try {
	    			
	    			//System.out.println(myImage.imageRGBMatrix[i][j]);
	    			Color c = new Color(myImage.imageRGBMatrix[i][j]);
	    			
	    			int red = c.getRed();
	    			int green = c.getGreen();
	    			int blue = c.getBlue();
	    			
	    			//System.out.println("red " + red + " GREEN " + green + " BLUE " + blue);
	    			BigInteger bRed = helper.valueOf(red);
	    			BigInteger bGreen = helper.valueOf(green);
	    			BigInteger bBlue = helper.valueOf(blue);
	    			
	    			//bRed = paillier.encrypt(BigInteger.valueOf(red));
	    			//bGreen = paillier.encrypt(BigInteger.valueOf(green));
	    			//bBlue = paillier.encrypt(BigInteger.valueOf(blue));
	    			
	    			//System.out.println("RED " + bRed + " GREEN " + bGreen + " BLUE " + bBlue);
	    			
	    			EncryptedPoint eP = new EncryptedPoint(helper.valueOf(red), helper.valueOf(green), helper.valueOf(blue));
	    			
	    			myImage.eI.points[i][j] = eP;
	    			
	    			//imageBigInteger[i][j] = paillier.encrypt(BigInteger.valueOf(imageRGBMatrix[i][j]));
	    		} catch (Exception e){
	    			e.printStackTrace();
	    		}
	    	}
	    	//System.out.println("@@");
	    }
	}
	
	public static void decryptMyImage(PaillierAlgorithm paillier) {
		
		myImage.decryptedImage = new int[myImage.eI.width][myImage.eI.height];
		
		for (int i = 0; i < myImage.eI.width; i++){
	    	for (int j = 0; j < myImage.eI.height; j++){
	    		try {
	    			int dRed = paillier.decrypt(myImage.eI.points[i][j].red).intValue();
	    			int dGreen = paillier.decrypt(myImage.eI.points[i][j].green).intValue();
	    			int dBlue = paillier.decrypt(myImage.eI.points[i][j].blue).intValue();
	    			//System.out.println(dRed);
	    			if(dRed > 255) {
	    				dRed = 255;
	    			}
	    			if(dRed < 0) {
	    				dRed = 0;
	    			}
	    			if(dGreen > 255) {
	    				dGreen = 255;
	    			}
	    			if(dGreen < 0) {
	    				dGreen = 0;
	    			}
	    			if(dBlue > 255) {
	    				dBlue = 255;
	    			}
	    			if(dBlue < 0) {
	    				dBlue = 0;
	    			}
	    			
	    			//System.out.println("Colors R - " + dRed + "  G - " + dGreen + " B - " + dBlue);
	    			
	    			Color c = new Color(dRed, dGreen, dBlue);
	    			//System.out.println("COLOR: " + c.getRGB());
	    			myImage.decryptedImage[i][j] = c.getRGB();
	    		} catch (Exception e){
	    			e.printStackTrace();
	    		}
	    	}
	    }
	}
	
	public static void main(String[] args) throws IOException { 
	
		if(args[0] != null){
			if(args[0].equals("trusted")){
				trusted = true;
			}
		}
		
		if(trusted) {
			
			System.out.println("This device is trusted");
		
		} else {
		
			System.out.println("This device is untrusted");
		
		}
		
		try { 
		
			Boolean flag = false;
			Boolean flag1 = false;
			Scanner scn = new Scanner(System.in); 
			
			// getting localhost ip 
			InetAddress ip = InetAddress.getByName("localhost"); 
	
			// establish the connection with server port 5056 
			Socket s = new Socket(ip, 5056); 
	
			// obtaining input and out streams 
			DataInputStream dis = new DataInputStream(s.getInputStream()); 
			DataOutputStream dos = new DataOutputStream(s.getOutputStream()); 
	
			while (true) { 
				
				System.out.println(dis.readUTF()); 
				String tosend = scn.nextLine(); 
				dos.writeUTF(tosend); 
				
				if(tosend.equals("Exit")) { 
					s.close(); 
					break; 
				} 
				String received = dis.readUTF(); 
				System.out.println(received);
				
				if(received.contains("so, you are trusted")){
				
					System.out.println("trusted response" + received);
					String paillierIncoming = FileUtils.readFile("./res/client/trusted/paillier.txt");//received.replace("so, you are trusted : ", "");
					paillier = JsonUtils.getPaillierFromJson(paillierIncoming);
					String incomingImage = FileUtils.readFile("./res/client/trusted/image.txt");	
					myImage.eI = JsonUtils.getMyEncryptedImageFromJson(incomingImage);//DecompressArray(incomingImage.getBytes()).toString());
					decryptMyImage(paillier);
					myImage.imageRGBMatrix = MultiplyMatrix.processMatrix4(myImage.decryptedImage);
					generateEncryptedImage(paillier);
					String encryptedImageJson = JsonUtils.getMyEncryptedImageJson(myImage.eI);
					String encryptedImage = "trusted response: ";// +  encryptedImageJson;//new String(CompressArray(encryptedImageJson.getBytes()));
					FileUtils.writeToFile("server/trusted/image", encryptedImageJson);
                    dos.writeUTF(encryptedImage);
					
				} else if (received.contains("so, you are untrusted")){
					
					String incomingImage = FileUtils.readFile("./res/client/untrusted/image.txt");
					myImage.eI = JsonUtils.getMyEncryptedImageFromJson(incomingImage);//DecompressArray(incomingImage.getBytes()).toString());
					myImage.eI.points = MultiplyMatrix.processMatrix3(myImage.eI.points);
					String encryptedImageJson = JsonUtils.getMyEncryptedImageJson(myImage.eI);//new String(CompressArray(getMyEncryptedImageJson().getBytes()));
					String encryptedImage = "untrusted response: ";// +  encryptedImageJson;
                    FileUtils.writeToFile("server/untrusted/image", encryptedImageJson);
                    dos.writeUTF(encryptedImage);
					
				}
			} 
			
			// closing resources 
			scn.close(); 
			dis.close(); 
			dos.close(); 
			
		} catch(Exception e) { 
			e.printStackTrace(); 
		} 
	} 
} 