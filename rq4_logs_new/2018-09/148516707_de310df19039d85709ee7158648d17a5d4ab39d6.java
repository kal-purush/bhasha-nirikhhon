/* 
 * The MIT License
 *
 * Copyright 2018 20182191.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package nl.knokko.util.socket.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import nl.knokko.util.bits.BitHelper;
import nl.knokko.util.bits.BitOutput;
import nl.knokko.util.bits.ByteArrayBitInput;
import nl.knokko.util.bits.ByteArrayBitOutput;
import nl.knokko.util.protocol.BitProtocol;
import nl.knokko.util.random.PseudoRandom;

public abstract class TCPServerSocket<State> implements Runnable {
	
	private int port;
	
	private ServerSocket socket;
	
	private boolean isStopping;
	
	private final BitProtocol<Handler> listener;
	
	private final Collection<Handler> clientHandlers;
	
	private final PseudoRandom random;

	public TCPServerSocket(BitProtocol<Handler> listener) {
		this.listener = listener;
		clientHandlers = new ArrayList<Handler>();
		random = new PseudoRandom();
	}
	
	public void start(int port){
		this.port = port;
		new Thread(this).start();
	}
	
	public void stop(){
		isStopping = true;
		try {
			socket.close();
		} catch(IOException ioex){}
	}
	
	@Override
	public void run(){
		try {
			socket = new ServerSocket(port);
			onOpen();
			while(true){
				Socket client = socket.accept();
				if(shouldAccept(client.getInetAddress().getAddress())){
                                    Handler handler = new Handler(client, createState());
                                    clientHandlers.add(handler);
                                    new Thread(handler).start();
				}
			}
		} catch(IOException ioex){
			if(!isStopping)
				onError(ioex);
		}
		onClose();
		synchronized(clientHandlers){
			for(Handler client : clientHandlers){
				try {
					client.socket.close();
				} catch(IOException ioex){}
			}
			clientHandlers.clear();
		}
	}
	
	public boolean isOnline(){
		return socket != null && !socket.isClosed();
	}
	
	public int getLocalPort(){
		return port;
	}
	
	public byte[] getAddress(){
		return socket.getInetAddress().getAddress();
	}
	
	protected abstract State createState();
	
	protected abstract boolean shouldAccept(byte[] clientAddress);
	
	protected abstract void onOpen();
	
	protected abstract void onError(IOException ioex);
	
	protected abstract void onClose();
	
	protected abstract void onHandlerOpen(Handler handler);
	
	protected abstract void onHandlerError(Handler handler, IOException ioex);
	
	protected abstract void onHandlerClose(Handler handler);
	
	public class Handler implements Runnable {
		
		private final Socket socket;
		private final State state;
		
		private OutputStream output;
		
		public Handler(Socket socket, State state){
			this.socket = socket;
			this.state = state;
		}
		
		public State getState(){
			return state;
		}
		
		public Socket getClient(){
			return socket;
		}

		@Override
		public void run() {
			try {
				InputStream input = socket.getInputStream();
				output = socket.getOutputStream();
				
				//verify that the address specified by the client is its real address by sending random bytes and asking the client to return the same bytes
				byte[] addressCheck = random.nextBytes(8);
				output.write(addressCheck);
				byte[] response = new byte[8];
				input.read(response);
				if(!Arrays.equals(addressCheck, response)) socket.close();
				onHandlerOpen(this);
				
				int size1 = input.read();
				int size;
				while(size1 != -1){
					size1++;//no 0 length messages
					if(size1 == 255){//next 2 bytes determine length
						size = 1 + BitHelper.makeChar((byte) input.read(), (byte) input.read());
					}
					else if(size1 == 256){//next 4 bytes determine length
						size = BitHelper.makeInt((byte) input.read(), (byte) input.read(), (byte) input.read(), (byte) input.read());
					}
					else {//length is smaller than 255
						size = size1;
					}
					byte[] inputBytes = new byte[size];
					input.read(inputBytes);
					listener.process(new ByteArrayBitInput(inputBytes), this);
					size1 = input.read();
				}
			} catch(IOException ioex){
				onHandlerError(this, ioex);
			} catch(Exception ex){
				System.out.println("A TCP socket was closed because an exception was thrown: " + ex.getMessage());
                                ex.printStackTrace();
			}
			onHandlerClose(this);
			synchronized(clientHandlers){
				clientHandlers.remove(this);
			}
		}
		
		public boolean isConnected(){
			return socket != null && socket.isConnected() && !socket.isClosed();
		}
		
		public BitOutput createOutput(){
			return new Output();
		}
		
		public void stop(String reason){
			try {
				socket.close();
			} catch(IOException ioex){}
			System.out.println("Closed connection because: " + reason);
		}
		
		private class Output extends ByteArrayBitOutput {
			
			@Override
			public void terminate(){
				try {
					byte[] messageBytes = getBytes();
					if(messageBytes.length == 0) throw new IllegalStateException("Empty messages are not supported");
					if(messageBytes.length < 255){
						output.write(messageBytes.length - 1);
					}
					else if(messageBytes.length <= Character.MAX_VALUE + 1){
						char c = (char) (messageBytes.length - 1);
						output.write(new byte[]{BitHelper.char0(c), BitHelper.char1(c)});
					}
					else {
						output.write(new byte[]{BitHelper.int0(messageBytes.length), BitHelper.int1(messageBytes.length), BitHelper.int2(messageBytes.length), BitHelper.int3(messageBytes.length)});
					}
					output.write(messageBytes);
				} catch(IOException ioex){
					onError(ioex);
				}
			}
		}
	}
}