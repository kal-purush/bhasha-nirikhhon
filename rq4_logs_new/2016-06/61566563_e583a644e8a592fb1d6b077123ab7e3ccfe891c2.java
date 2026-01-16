package hu.barbar.comm.client;

import java.io.IOException;
import java.net.Socket;

import hu.barbar.comm.util.Commands;

public abstract class Client extends Thread{

	public static final int versionCode = 101;
	public static final String version = "1.0.1";
	
	
	protected int TIMEOUT_WAIT_WHILE_IS_OK_IN_MS = 1000;
			
	private Client me;
	
	private String host = null;
	private int port = 0;
	
	private Socket socket = null;
	private boolean connected = false;
	
	private ClientThread myClientThread = null;

	public Client() {
		super();
		me = Client.this;
	}
	
	public Client(String host, int port) {
		super();
		this.host = host;
		this.port = port;
		connected = false;
		me = Client.this;
	}
	
	public Client(String host, int port, int timeOutForIsOK) {
		super();
		this.host = host;
		this.port = port;
		connected = false;
		me = Client.this;
		this.TIMEOUT_WAIT_WHILE_IS_OK_IN_MS = timeOutForIsOK;
	}
	
	@Override
	public void run() {
		this.connect();
		super.run();
	}
	
	public boolean connect(){
		
		if (host == null) {
			showOutput("hostname is null");
			connected = false;
			return false;
		}
		
		try {
			/**
			 * Connect to Server
			 */
			socket = new Socket(host, port);
			if(socket.isConnected()){
				//showOutput("Socket is connected");
				
				myClientThread = new ClientThread(socket) {
					
					@Override
					public boolean handleReceivedMessage(String message) {
						if(me != null){
							me.handleRecievedMessage(message);
							return true;
						}else{
							return false;
						}
						
					}
				};
				//Thread.sleep(100);
				myClientThread.start();
				
			}else{
				//TODO
			}
			
			showOutput("CLIENT: Connected to server " + host + " @ " + port);
			connected = true;
			//Client.this.onConnected();
			return true;
			
		} catch (Exception e) {
			showOutput("Can not establish connection to " + host + " @ " + port);
			e.printStackTrace();
			showOutput(e.toString());
			connected = false;
			return false;
		}
		
	}
	
	public void onConnected() {}

	public void onDisconnected(){}
	
	public void disconnect(){
		if(this.isConnected()){
			sendMessage(Commands.CLIENT_EXIT);
			myClientThread.disconnect();
			try {
				socket.close();
			} catch (IOException e) {}
			try{
				myClientThread.interrupt();
			}catch(Exception e){
			}finally{
				onDisconnected();
			}
		}
		
	}
	
	public boolean sendMessage(String message){
		boolean retVal = false;
		if(myClientThread != null){
			if(message != null){
				retVal = myClientThread.sendMessageToServer(message);
			}else{
				showOutput("Can not send message: message is null");
			}
		}else{
			showOutput("Can not send message: clientThread is null");
		}
		return retVal;
	}
	
	protected abstract void handleRecievedMessage(String message);
	
	public boolean waitWhileIsOK(){
		return this.waitWhileIsOK(TIMEOUT_WAIT_WHILE_IS_OK_IN_MS);
	}
	
	public boolean waitWhileIsOK(int timeoutInMs){
		
		if(this.isOK()){
			return true;
		}
		
		int attempCount = 0;
		int maxAttemps = (timeoutInMs / 50);
		if (maxAttemps < 1){
			maxAttemps = 1;
		}
		while(this.isOK() == false && attempCount < maxAttemps){
			attempCount++;
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {}
		}
		
		return this.isOK();
	}
	
	protected abstract void showOutput(String text);


	public boolean isConnected(){
		if (socket == null){
			return false;
		}
		return socket.isConnected() && this.connected;
	}


	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}


	public boolean setHost(String host) {
		if(socket == null || !socket.isConnected()){
			this.host = host;
			return true;
		}
		return false;
	}

	public boolean setPort(int port) {
		if(socket == null || !socket.isConnected()){
			this.port = port;
			return true;
		}
		return false;
	}

	
	public boolean isOK() {
		return (myClientThread != null && myClientThread.isOK());
	}

	public static int getVersioncode() {
		return versionCode;
	}

	public static String getVersion() {
		return version;
	}

}