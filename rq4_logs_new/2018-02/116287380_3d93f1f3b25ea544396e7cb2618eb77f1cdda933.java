package com.p2pble;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

/**
 * Created by rajeshmaheswaran on 08/02/18.
 */

public class UdpServerThread2 extends Thread {
    int serverPort;
    int[] ports;
    DatagramSocket socket;

    boolean running;
    public UdpServerThread2(int serverPort,int [] obj) {
        super();
        this.serverPort = serverPort;
        ports=obj;
        //for(int i :obj)
        //System.out.println(i);

    }

    public void run() {


        try {
            socket = new DatagramSocket(serverPort);
        } catch (SocketException e) {
            e.printStackTrace();
        }
        running = true;

        HashMap<String,Timestamp> msg_time = new HashMap<String,Timestamp>();

        try {

            while(running){
                byte[] buf = new byte[1024];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);

                socket.receive(packet);     //this code block the program flow
                System.out.println("server call");
                InetAddress address = packet.getAddress();
                int port = packet.getPort();
                String msg =   new String(packet.getData());
                //System.out.println("message - "+msg);
                String[] temp = msg.split("_");
                String message = temp[0];
                String timer = temp[1];
                System.out.println("message is "+message);
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
                Date parseDate = null;
                try {
                    parseDate = dateFormat.parse(timer);
                } catch (ParseException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                Timestamp timestamper = new java.sql.Timestamp(parseDate.getTime());//Convert string into timestamp
                int rec_time = timestamper.getSeconds();
                /*Set<String> keys = msg_time.keySet();
                Iterator<String> ite = keys.iterator();
                while(ite.hasNext()){
                	String key = ite.next();
                	if(key.equals(message)){*/
                Timestamp time_val = msg_time.get(message);
                if(time_val==null)
                {
                    msg_time.put(message,timestamper);
                    for(int i:ports){
                        String new_msg=message+"_"+timestamper;
                        UdpClientThread send = new UdpClientThread(new_msg.getBytes(),"",i);
                        send.start();
                        System.out.println("Packets are forwarded again");
                    }
                }
                else
                {
                    int t = time_val.getSeconds();
                    if(rec_time>t){
                        msg_time.put(message,timestamper);
                        for(int i:ports){
                            String new_msg=message+"_"+timestamper;
                            UdpClientThread send = new UdpClientThread(new_msg.getBytes(),"",i);

                            send.start();
                            System.out.println("Packets are forwarded again");
                        }
                    }
                }
                System.out.println("Hastable---- " + msg_time.toString());
            }
            //   msg_time.put(message, timestamper);





        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(socket != null){
                socket.close();

            }
        }
    }
}