package co.edu.icesi;

import java.io.*;
import java.net.Socket;
import java.util.Base64;

/**
 * The SecureClient class represents a client that uses the Diffie-Hellman key exchange
 * protocol for secure communication with a server.
 */
public class SecureClient {

    private Socket serverSocket;
    private BufferedReader serverBufferedReader;
    private BufferedWriter serverBufferedWriter;
    private SecureCommunicationManager secureCommManager;
    private Thread listenerThread;

    /**
     * Constructs a SecureClient object with the specified Socket for connecting to a server.
     *
     * @param serverSocket The Socket used to connect to the server.
     * @throws IOException If an I/O error occurs while setting up the client.
     */
    public SecureClient(Socket serverSocket) throws IOException {
        this.serverSocket = serverSocket;
        this.serverBufferedReader = new BufferedReader(new InputStreamReader(this.serverSocket.getInputStream()));
        this.serverBufferedWriter = new BufferedWriter(new OutputStreamWriter(this.serverSocket.getOutputStream()));
        this.secureCommManager = new SecureCommunicationManager();

        secureCommManager.generateKeyPair();

        System.out.println("Connected to server at " + serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getLocalPort());
    }

    /**
     * Listens for incoming messages from the server and processes them accordingly.
     */
    public void receiveMessageFromServer() {
        System.out.println("Listening for server incoming messages...");
        listenerThread = new Thread(() -> {
            while (serverSocket.isConnected()) {
                try {
                    String messageFromServer = serverBufferedReader.readLine();

                    if(messageFromServer.contains("SYN/ACK ")){
                        secureCommManager.receivePublicKeyFromOtherParty(messageFromServer.replaceAll("SYN/ACK ", ""));
                        sendMessageToServer("ACK ");
                    } else {
                        String decryptedMessage = secureCommManager.decodeAndDecryptMessage(messageFromServer);
                        ClientController.showMessage(decryptedMessage);
                    }
                } catch (IOException ioe) {
                    System.out.println("Error receiving message from server.");
                    ioe.printStackTrace();
                    break;
                }
            }
        });

        listenerThread.start();
    }

    /**
     * Gets the public key of the client.
     *
     * @return The Base64-encoded public key.
     */
    public String getEncodedPublicKey(){
        return Base64.getEncoder().encodeToString(secureCommManager.getOwnPublicKey().getEncoded());
    }

    /**
     * Sends a message to the connected server.
     *
     * @param messageToSend The message to be sent.
     */
    public void sendMessageToServer(String messageToSend) {
        try {
            String encryptedMsg = messageToSend;
            if(!messageToSend.contains("SYN ") & !messageToSend.contains("ACK ")){
                encryptedMsg = new String(secureCommManager.encryptAndEncodeMessage(messageToSend));
            }
            serverBufferedWriter.write(encryptedMsg);
            serverBufferedWriter.newLine();
            serverBufferedWriter.flush();
        } catch (IOException ioe) {
            System.out.println("Error sending message to the server.");
            ioe.printStackTrace();
            closeConnections(serverSocket, serverBufferedReader, serverBufferedWriter);
        }
        System.out.println("Message sent to server");
    }

    /**
     * Closes all the open connections including the Socket, BufferedReader, and BufferedWriter.
     *
     * @param serverSocket The Socket to be closed.
     * @param serverBufferedReader The BufferedReader to be closed.
     * @param serverBufferedWriter The BufferedWriter to be closed.
     */
    public void closeConnections(Socket serverSocket, BufferedReader serverBufferedReader, BufferedWriter serverBufferedWriter) {
        try {
            if (serverBufferedReader != null) serverBufferedReader.close();
            if (serverBufferedWriter != null) serverBufferedWriter.close();
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    /**
     * Gets the listener thread associated with this SecureClient.
     *
     * @return The listener thread.
     */
    public Thread getListenerThread() {
        return listenerThread;
    }

    /**
     * Gets the Socket associated with this SecureClient.
     *
     * @return The Socket.
     */
    public Socket getServerSocket() {
        return serverSocket;
    }

    /**
     * Gets the BufferedReader associated with this SecureClient.
     *
     * @return The BufferedReader.
     */
    public BufferedReader getServerBufferedReader() {
        return serverBufferedReader;
    }

    /**
     * Gets the BufferedWriter associated with this SecureClient.
     *
     * @return The BufferedWriter.
     */
    public BufferedWriter getServerBufferedWriter() {
        return serverBufferedWriter;
    }
}