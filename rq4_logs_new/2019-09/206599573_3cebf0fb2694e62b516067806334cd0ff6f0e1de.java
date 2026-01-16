import java.util.*;
import java.io.IOException;
import java.io.FileOutputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.net.SocketTimeoutException;
import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import java.net.MulticastSocket;


public class Peer {
    private String directory; // unique directory for each peer
    private int port; // unique port for each peer
    private String name; // unique name for each peer
    private static int broadcastPort = 5000; // this port is bond to broadcast
    HashMap<String,String> files; // saving name and location of files
    private static DatagramSocket socket = null; // this socket is used when we want to broadcast
    private Server server; // dedicated thread


    /**
     * constructor
     * @param name is the name of the peer
     * @param port is the port of the peer
     */
    Peer(String name, String port){
        this.name = name;
        this.port = Integer.parseInt(port);
        directory = System.getProperty("user.dir")+"/../Dirs/" + name + "/";    // the directory to save files
        /* TODO: create Dirs directory next to the folder containing this code and then create proper subfolders
            for each peer if it does not exist
        */
        files = new HashMap<>();
        server = new Server(broadcastPort, this); // all the peers are listening to the broadcast port
        server.start(); // starts listening to the broadcast port
    }

    String getDirectory() { return this.directory; }

    int getPort(){ return this.port; }

    String getName() { return this.name; }


    /**
     * shows the peer's files name and location
     */
    void showFiles(){
        if(files.size() > 0) {
            for (Map.Entry m : files.entrySet()) {
                System.out.println(m.getKey() + " " + m.getValue());
            }
        }
    }


    /**
     *
     * @param broadcastMessage contains the desired file
     * @param address ip to create packet
     * @param peer is sent to get its port
     * @throws IOException if anything went wrong
     */
    static void broadcast(String broadcastMessage, InetAddress address, Peer peer) throws IOException {
        socket = new DatagramSocket();
        socket.setBroadcast(true);
        byte[] buffer = (broadcastMessage + " " + peer.getPort()).getBytes();
        DatagramPacket packet
                = new DatagramPacket(buffer, buffer.length, address, broadcastPort);

        new Client(peer);  // this peer catches all the offers from other peers who have desired file
        socket.send(packet);
        socket.close();

    }

    public static void main(String[] args) throws IOException{
        /* TODO: run Peer.java in terminal with two arguments name and port
            java Peer.java 1 8080
         */
        Peer p = new Peer(args[0], args[1]);
        System.out.println("New peer joined!");

        Scanner scan = new Scanner(System.in);
        String input = scan.nextLine();


        while(!input.equals("terminate")){
            if(input.startsWith("p2p -serve")){

                String[] parts = input.split(" ");
                System.out.println(p.directory);

                /* TODO: if you want to specify the path for each part comment out the line below
                    sample input:
                    p2p -serve -name <file_name> -path <file_path>
                */
                //p.files.put(parts[3], parts[5]);

                /* TODO: otherwise sample input will be like:
                    p2p -serve -name <file_name>
                    and file path will be the directory of the peer
                 */
                p.files.put(parts[3], p.directory);
                p.showFiles(); //showing all the files that the peer can serve
            }
            else if (input.startsWith("p2p -receive")){  // p2p -receive hello.txt
                broadcast(input.split(" ")[2], InetAddress.getByName("255.255.255.255"), p);
            }
            else if(input.equals("p2p -show")){
                p.showFiles();
            }

            input = scan.nextLine();
        }

        p.server.terminate(); // terminates the peer's thread
    }

}

class Server extends Thread {
    private byte[] receive;
    private byte[] res; // result
    private int port;
    private int portBroadcast;
    DatagramPacket DpReceive = null;
    MulticastSocket socket;
    Peer peer;
    private boolean run = true;
    private boolean receiveFlag = true;

    public Server(int port, Peer peer){
        this.peer = peer;
        this.portBroadcast = port;
        this.port = peer.getPort();

        try {
            socket = new MulticastSocket(portBroadcast);
        }catch (SocketException e){}
        catch (IOException e){
            System.out.println(e.getMessage());
        }

        receive = new byte[65535];
    }

    public void terminate(){
        System.out.println("Server: Thread terminated");
        run = false;
        receiveFlag = false;
    }

    /**
     * These commands will be executed in new thread
     */
    public void run(){


        while (run)
        {
            receiveFlag = true;
            DpReceive = new DatagramPacket(receive, receive.length);
            System.out.println("Server: The peer is listening to the broadcast messages");

            try {
                socket.setSoTimeout(3000); // checks thread status
            } catch (SocketException e) {
                System.out.println("Server: Socket Exception Occurred");
            }



            while(receiveFlag){ // while the thread is alive
                try {
                    socket.receive(DpReceive);
                    System.out.println("Server: Broadcast message received");
                    receiveFlag = false;
                } catch (IOException e){
                    //System.out.println("Time out");
                }
            }
            if(!run)
                break; // close command entered by the peer

            InetAddress address = DpReceive.getAddress();

            //processing the broadcast message
            System.out.println("Server: message = " + appendByte(receive).toString());
            int portReceived = Integer.parseInt(appendByte(receive).toString().split(" ")[1]);
            String fileName = appendByte(receive).toString().split(" ")[0];

            // avoid sending broadcast message to itself
            if (peer.files.containsKey(fileName) && !(portReceived == port)){

                new Sender(peer);
                System.out.println("Server: File Found :) = " + fileName + " " + peer.getPort());
                res = new byte[65535];
                res = (fileName + " " + peer.getPort()).getBytes(); // [fileName + portNumber] to Client
                DatagramPacket dSend = new DatagramPacket(res, res.length, address, portReceived);
                try {
                    socket.send(dSend);
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                }
            }
            else
                System.out.println("Server: Sorry I don't have this file !");

            receive = new byte[65535];
        }

        socket.close();
        System.out.println("Server: broadcast socket closed");
    }


    public static StringBuilder appendByte(byte[] a) {
        if (a != null) {
            StringBuilder ret = new StringBuilder();
            int i = 0;
            while (a[i] != 0) {
                ret.append((char) a[i]);
                i++;
            }
            return ret;
        }
        return null;
    }
}

class Client extends Thread {

    private Peer peer; // create a client from a peer
    private DatagramSocket ds;
    private DatagramPacket DPSend = null;
    private int port;
    InetAddress ip;
    private byte[] receive; // receive buffer
    private byte[] send; // sent buffer

    Client(Peer peer) throws UnknownHostException{
        receive = new byte[65535]; //maximum size of 2 to the power of 16
        this.peer = peer;

        this.port = peer.getPort();
        try {
            ds = new DatagramSocket(port);
            ds.setSoTimeout(1000); // in case that requested file does not exist in network
        } catch (SocketException e) {
            System.out.println("Client: Socket Exception Occurred: " + e.getMessage());
        }
        ip = InetAddress.getLocalHost();
        this.start();
    }

    /**
     * these commands will be executed in new thread
     */
    public void run(){
        receive = new byte[65535];
        send = new byte[65535];
        receive[0] = 22;

        System.out.println("Client: Waiting for other peers to respond");
        DatagramPacket receivePacket =
                new DatagramPacket(receive, receive.length);
        try{
            ds.receive(receivePacket);
        } catch (IOException e){
            System.out.println("Client: first try timeout: " + e.getMessage());
        } catch (NullPointerException e){
            System.out.println("Client: first try null reference: " + e.getMessage());
        }


        try{
            if(receive[0] != 22) { // second try

                ds.close(); // close datagram socket and create new one
                ds = new DatagramSocket(port);

                System.out.println("Client: Response Received = " + appendByte(receive).toString());
                String fileName = appendByte(receive).toString().split(" ")[0];
                String portPacket = appendByte(receive).toString().split(" ")[1];
                InetAddress address = receivePacket.getAddress();

                send = (fileName + " " + peer.getPort()).getBytes();

                DPSend = new DatagramPacket(send, send.length, address, Integer.parseInt(portPacket)); // to Sender
                ds.send(DPSend);

                receive = new byte[65535];
                receivePacket = new DatagramPacket(receive, receive.length);
                try{
                    ds.receive(receivePacket);
                } catch (IOException e){
                    System.out.println("Client: second try timeout: " + e.getMessage());
                } catch (NullPointerException e){
                    System.out.println("Client: second try null reference: " + e.getMessage());
                }

                System.out.println("Client: receiving completed: " + appendByte(receive).toString());
                String[] file = appendByte(receive).toString().split(" ");
                String f = file[0];
                String[] check = appendByte(receive).toString().split(" ");
                if(check.length == 1){
                    peer.files.put(f, peer.getDirectory());
                    receiveFile(ds, f, peer.getDirectory(), port, address);
                }
                else {
                    System.out.println("UDP connection failed");
                }



            }

        } catch (NullPointerException e){
            System.out.println("Client: error getting the file" + e.getMessage());
        }  catch (IOException e) {
            System.out.println(e.getMessage());
        }

        try {
            ds.close();
            System.out.println("Client: datagram socket closed successfully");
        } catch (NullPointerException e){
            System.out.println("Client: error closing datagram socket: " + e.getMessage());
        }

        System.out.println("Client: action done");
    }

    public static void receiveFile(DatagramSocket ds, String fileName, String dir, int portReceive, InetAddress ip) {
        System.out.println("receiveFile method executed");
        System.out.println("ip = " + ip);
        System.out.println("port = " + portReceive);
        String directory = dir + fileName;
        DatagramSocket socketReceive = ds;

        int n = 6;
        byte[] fileLength = new byte[n];
        DatagramPacket DpReceive = new DatagramPacket(fileLength, fileLength.length);

        try {
            socketReceive.receive(DpReceive); // receiving size of the requested file
        }catch (IOException e){
            System.out.println(e.getMessage());
        }

        int fileSize = 0;
        for(int i = 0; i < n; i++) // changing base of the size from 128 to 10
            fileSize += fileLength[i] * (int)Math.pow(128,i);

        // assembling packets to form a file
        System.out.println("Requested File Size = " + fileSize);
        int maxSize = 65000;
        System.out.println("Max Size sending via UDP = " + maxSize);
        int numberOfPackets = fileSize/maxSize + 1;
        System.out.println("Number of packets = " + numberOfPackets);
        int lastPacketSize = fileSize%maxSize;
        System.out.println("Last packet size = " + lastPacketSize);

        byte[] fileContent = new byte[fileSize];

        int lossCounter = 0; // number of packet loss
        int t = numberOfPackets / 12;
        t += 6;
        try {
            socketReceive.setSoTimeout(t);
        }catch (SocketException e){
            System.out.println(e.getMessage());
        }

        for(int i = 0; i < (numberOfPackets - 1); i++) { // receiving packets
            System.out.println("Packet " + i + " is receiving ...");
            byte[] arr = new byte[maxSize];
            DpReceive = new DatagramPacket(arr, arr.length);
            try{
                socketReceive.receive(DpReceive);
            }catch (SocketTimeoutException e){
                System.out.println("Packet Loss occurred!");
                lossCounter++;
            }catch (IOException e){
                System.out.println(e.getMessage());
            }
            for(int j = 0; j < maxSize; j++){
                fileContent[j + i*maxSize] = arr[j]; // appending packets
            }
        }

        byte[] arr = new byte[lastPacketSize];
        DpReceive = new DatagramPacket(arr, arr.length);

        try{
            socketReceive.receive(DpReceive); // receiving the last packet
        }catch (SocketTimeoutException e){
            System.out.println("Last packet Loss!");
            lossCounter++;
        }catch (IOException e){
            System.out.println(e.getMessage());
        }

        socketReceive.close(); // closing the socket

        for(int i = 0; i < fileSize%maxSize; i++){
            fileContent[i + (numberOfPackets - 1) * maxSize] = arr[i]; // appending the last packet
        }
        try (FileOutputStream fos = new FileOutputStream(directory)) {
            fos.write(fileContent);
        } catch (IOException e){}
        System.out.println(lossCounter + " packets lost");
    }

    public static StringBuilder appendByte(byte[] a) {
        if (a != null) {
            StringBuilder ret = new StringBuilder();
            int i = 0;
            while (a[i] != 0) {
                ret.append((char) a[i]);
                i++;
            }
            return ret;
        }
        return null;
    }

}

class Sender extends Thread {

    private Peer peer; // create sender from a peer
    private int port;
    private DatagramSocket ds;
    DatagramPacket DpReceive = null;
    DatagramPacket DpSend = null;
    private byte[] receive; // receive buffer
    private byte[] send; // sent buffer

    public Sender(Peer peer){
        this.peer = peer;
        this.port = peer.getPort();
        this.start();
    }

    /**
     * these commands will be executed in new thread
     */
    public void run(){

        receive = new byte[65535];
        send = new byte[65535];
        DpReceive = new DatagramPacket(receive, receive.length);

        System.out.println("Sender: New Sender is admitted");

        try {
            ds = new DatagramSocket(port);
            ds.setSoTimeout(1000); // when file has sent by another peer
            ds.receive(DpReceive);
            System.out.println("Sender: Request message received = " + appendByte(receive).toString());

            String fileName = appendByte(receive).toString().split(" ")[0];
            String portPacket = appendByte(receive).toString().split(" ")[1];
            InetAddress address = DpReceive.getAddress();
            send = (fileName).getBytes();
            DpSend = new DatagramPacket(send, send.length, address, Integer.parseInt(portPacket));

            ds.send(DpSend);
            System.out.println("Sender: File Sent = " + fileName);

            sendFile(ds, fileName, peer.getDirectory(), Integer.parseInt(portPacket), address);
            System.out.println("Sender: done");
            ds.close();
            System.out.println("Sender: socket closed");

        } catch (IOException e){
            System.out.println("Sender: Timeout" + e.getMessage());
            ds.close();
        }


    }


    public static void sendFile(DatagramSocket ds, String fileName, String address, int portSend, InetAddress ip) {
        System.out.println("sendFile method invoked");
        System.out.println("IP = " + ip);
        System.out.println("Port = " + portSend);

        String directory = address + fileName;
        File file;
        file = new File(directory);
        byte[] fileContent = null;

        try {
            fileContent = Files.readAllBytes(file.toPath());
        } catch (IOException e){}

        //System.out.println("S1");
        int fileSize = fileContent.length;
        System.out.println("Requested File Size = " + fileSize);
        int maxSize = 65000;
        System.out.println("Max Size sending via UDP = " + maxSize);
        int numberOfPackets = fileSize/maxSize + 1;
        System.out.println("Number of packets = " + numberOfPackets);
        int lastPacketSize = fileSize%maxSize;
        System.out.println("Last packet size = " + lastPacketSize);
        int t = numberOfPackets / 40;
        t += 3;

        int n = 6;
        byte[] fileLength = new byte[6];

        n--;
        while ( n >= 0 ){ // changing base of the size from 10 to 128
            fileLength[n] = (byte) (fileSize / (int)Math.pow(128,n));
            fileSize -= fileLength[n] * (int)Math.pow(128,n);
            n--;
        }

        DatagramPacket DpSend =
                new DatagramPacket(fileLength, fileLength.length, ip, portSend);
        try {
            ds.send(DpSend);
        } catch (IOException e){
            System.out.println("Sender IO problem2" + e.getMessage());
        }

        for(int i = 0; i < (numberOfPackets - 1); i++) { // sending packets
            byte[] arr = new byte[maxSize];
            for (int j = 0; j < maxSize; j++) {
                arr[j] = fileContent[j + i * maxSize];
            }

            try {
                sleep(t);
            }
            catch (InterruptedException e) {
                System.out.println("got interrupted!");
            }

            DpSend = new DatagramPacket(arr, arr.length, ip, portSend);
            try {
                ds.send(DpSend);
            }
            catch (IOException e){
                System.out.println("Sender IO problem3" + e.getMessage());
            }
        }
        try {
            sleep(t);
        }
        catch (InterruptedException e) {
            System.out.println("got interrupted!");
        }

        byte[] arr = new byte[lastPacketSize];
        for(int i = 0; i < lastPacketSize; i++){
            arr[i] = fileContent[i + (numberOfPackets - 1) * maxSize];
        }
        DpSend = new DatagramPacket(arr, arr.length, ip, portSend);

        try {
            ds.send(DpSend);
        }
        catch (IOException e){
            System.out.println("Sender IO problem4");
        }
    }

    public static void sleep(int t) throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(t);
    }

    public static StringBuilder appendByte(byte[] a) {
        if (a != null) {
            StringBuilder ret = new StringBuilder();
            int i = 0;
            while (a[i] != 0) {
                ret.append((char) a[i]);
                i++;
            }
            return ret;
        }
        return null;
    }
}