package ru.sandem.java3.lesson7.client;

import ru.sandem.java3.lesson7.utils.Properties;

import java.io.*;
import java.net.Socket;
import java.text.MessageFormat;

public class ChatClient implements Runnable{
    private Socket socket;
    private String clientName;

    private BufferedReader socketReader;
    private BufferedWriter socketWriter;

    private PrintStream outPrintStream;
    private boolean CLIENT_IS_CLOSED = false;

    public String getClientName() {
        return clientName;
    }

    public ChatClient(String host, int port, String clientName) {
        try {
            this.clientName = clientName;
            socket = new Socket(host, port);
            socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            socketWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        } catch (IOException e) {
            System.err.println(MessageFormat.format(
                    "Проблема с подключением к сокету: {0}:{1}", host, port));
        }
    }

    public void setCLIENT_IS_CLOSED(boolean CLIENT_IS_CLOSED) {
        this.CLIENT_IS_CLOSED = CLIENT_IS_CLOSED;
    }

    @Override
    public void run() {
        try{
            while (true) {
                String message = socketReader.readLine();
                outPrintStream.println(message);
            }
        } catch (IOException e) {
            if (CLIENT_IS_CLOSED) {
                return;
            } else {
                System.err.println("Ошибка на клиенте: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public void sendMessage(String message) {
        try{
            if (Properties.HALT_MESSAGE.equalsIgnoreCase(message)){
                socketWriter.write(message);
            } else if (clientName.equals(message)) {
                socketWriter.write("/id " + message + "\n");
            } else {
                socketWriter.write(clientName + ": " + message + "\n");
            }
            socketWriter.flush();
        } catch (IOException e) {
            System.err.println("Ошибка при отправке сообщения: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public PrintStream getOutPrintStream() {
        return outPrintStream;
    }

    public void setOutPrintStream(PrintStream outPrintStream) {
        this.outPrintStream = outPrintStream;
    }

    public BufferedWriter getSocketWriter() {
        return socketWriter;
    }

    public BufferedReader getSocketReader() {
        return socketReader;
    }

    public Socket getSocket() {
        return socket;
    }
}