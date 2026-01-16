package group6;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Scanner;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import java.sql.ResultSet;

public class Database {

	private static final int PORT = 2047;
	private static int value;
	private String time;
	static String url="jdbc:sqlite:C:/Users/Pallavi Singh/git/SYSC-3010-project/AirQ.db";
	private DatagramPacket receivePacket;
	private DatagramSocket dataSocket;
	private byte[] receiveData;
	private int count;
	private int port;
	
	public Database(int port) throws IOException{
		this.port = port;
		this.receiveData = new byte[512];
		try {
			dataSocket = new DatagramSocket(port);
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
    public Connection connect() throws IOException  {
        Connection conn = null;
        try {
            // database parameters
            this.url = url;
            // create a connection to the database
            conn = DriverManager.getConnection(url);
            System.out.println("Connection to SQLite has been established.");
            run();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
         finally {
            try {
               if (conn != null) {
                    conn.close();
                }
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        }
		return conn;
    }
    
    public void insert(int value, String time) throws IOException {
    	this.value=value;
    	this.time=time;
        String sql = "INSERT INTO SensorData(value,time) VALUES(?,?)";
        System.out.println("inserting value: " +value);
        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
        	System.out.println("value inserted");
            // set the corresponding param
        	pstmt.setInt(1, value);
            pstmt.setString(2, time);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    
    public void delete() throws IOException {
        String sql = "DELETE FROM SensorData";
 
        try (Connection conn = this.connect();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    
    public void display() throws IOException{
        String sql = "SELECT value, time FROM SensorData";
        
        try (Connection conn = this.connect();
        		Statement stmt = conn.createStatement()) {
        	ResultSet rs    = stmt.executeQuery(sql);
            // loop through the result set
            while (rs.next()) {
                System.out.println(rs.getInt("Value") +  "\t" + rs.getString("Time") + "\t");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    /**
     * @throws IOException 
     */
    public void run() throws IOException{
    	System.out.println( "Receiving on port " + PORT ) ; 
        while(true){
        	receivePacket = new DatagramPacket(this.receiveData, this.receiveData.length);                 
			dataSocket.receive(receivePacket);
            String str = new String(receivePacket.getData()).trim();
            System.out.println(str);
          value=Integer.parseInt(str.toString()); 
          time=CurrentTime();
          //value=50;
          insert(value, time);
          //break;
          if(!dataSocket.isBound()){
			  Connection conn = this.connect();
			  try {
				if (conn != null) {
                    conn.close();
                }
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }
        	  break;
          }
        }
	    dataSocket.close();
    }
    
    
    /**
	 * 
	 * @param bytes
	 * @return
	 */
	public  static int[] bytesToInts(byte[] bytes) {
	    int[] ints = new int[bytes.length / 4];
	    ByteBuffer.wrap(bytes).asIntBuffer().get(ints);
	    return ints;
	}
	
	/**
	 * 
	 * @param ints
	 * @return
	 */
	public byte[] intsToBytes(int[] ints) {
	    ByteBuffer bb = ByteBuffer.allocate(ints.length * 4);
	    IntBuffer ib = bb.asIntBuffer();
	    for (int i : ints) ib.put(i);
	    return bb.array();
	}
	
	private String CurrentTime(){
		DateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
	    Date dateobj = new Date();
	    System.out.println(df.format(dateobj));
	    time=df.format(dateobj);
		return time;
	}
	
    public static void main(String[] args) throws Exception{
    	 Database d = new Database(PORT);
    	 d.connect();
    	 d.display();
         
    }
}

