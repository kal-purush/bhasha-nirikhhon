package hu.barbar.piclient;

import hu.barbar.comm.client.Client;
import hu.barbar.comm.util.Msg;
import hu.barbar.util.LogManager;

public abstract class CommunicationThread extends Thread {
	
	private static final int DEFAULT_TIMEOUT_FOR_CONNECTION_READY = 1000;
	
	private int timeOutForConnectionReadyInMs = DEFAULT_TIMEOUT_FOR_CONNECTION_READY;
	private String host = null;
	private int port = -1;
	
	private Client myClient = null;
	
	
	public CommunicationThread(String host, int port, int timeoutForReady) {
		this.timeOutForConnectionReadyInMs = timeoutForReady;
		this.host = host;
		this.port = port;
		init();
	}
	
	public CommunicationThread(String host, int port) {
		this.timeOutForConnectionReadyInMs = DEFAULT_TIMEOUT_FOR_CONNECTION_READY;
		this.host = host;
		this.port = port;
		init();
	}
	
	private void init(){
		myClient = new Client(host, port, 1000) {
			
			@Override
			protected void showOutput(String arg0) {
				showText(arg0);
			}
			
			public void onConnected(String host, int port) {
				CommunicationThread.this.onClientConnected(host, port);
			};
			
			public void onDisconnected(String host, int port) {
				//btnConnect.setText("Connect");
				CommunicationThread.this.onDisconnected(host, port);
			};
			
			@Override
			protected void handleRecievedMessage(Msg message) {
				//showText("Received: " + message.toString());
				CommunicationThread.this.handleRecievedMessage(message);
			}
		};
	}

	@Override
	public void run() {
		
		if(myClient != null){
			myClient.start();
			super.run();
		}else
			this.interrupt();
		
		
	}

	public synchronized boolean isConnected() {
		return (myClient != null && myClient.isConnected());
	}

	public synchronized boolean sendMessage(Msg message) {
		if(myClient != null){
			return myClient.sendMessage(message);
		}
		return false;
	}

	
	public abstract void showText(String text);
	
	public abstract void onClientConnected(String host, int port);
	
	protected abstract void handleRecievedMessage(Msg message);

	protected abstract void onDisconnected(String host2, int port2);

	public synchronized void disconnect() {
		if(myClient != null)
			myClient.disconnect();
	}


	public synchronized void setLogManager(LogManager log) {
		if(myClient != null)
			myClient.setLogManager(log);
	}
	
}