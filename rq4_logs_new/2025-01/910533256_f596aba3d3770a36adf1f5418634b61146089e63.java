package tictactoe.data;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.net.Socket;
import javafx.application.Platform;
import javafx.stage.Stage;
import javax.json.Json;
import javax.json.JsonObject;
import tictactoe.ui.alert.IncomingRequestDialog;

public class ConnectionService {

    private Socket server;
    private DataInputStream dis;
    private DataOutputStream dos;

    private static final String LOCAL_HOST = "127.0.0.1";
    private static final int PORT = 55555;
    private static final int ACTION_SIGN_UP = 1;
    private static final int ACTION_LOGIN = 2;
    private static final int ACTION_ONLINE_USERS = 3;
    private static final int ACTION_SEND_REQUEST = 4;
    private static final int ACTION_SEND_MOVE = 5;
    private static final int ACTION_LOGOUT = 6;

    private static volatile ConnectionService instance;
    private static Thread th;

    public static boolean running;

    public static ConnectionService getInstance() {
        if (instance == null) {
            instance = new ConnectionService();
        }
        return instance;
    }

    public boolean connect() {

        if (!isConnected()) {
            try {
                server = new Socket(LOCAL_HOST, PORT);
                dis = new DataInputStream(server.getInputStream());
                dos = new DataOutputStream(server.getOutputStream());
                startListening();
                return true;

            } catch (IOException ex) {
                System.out.println("error connecting to server" + ex.getMessage()); // TODO show Message to User
                return false;
            }
        }
        return true;
    }

    public void disconnect() {
        try {
            if (server != null) {
                server.close();
            }
            if (dis != null) {
                dis.close();
            }
            if (dos != null) {
                dos.close();
            }
            System.out.println("disconnected from server");
        } catch (IOException ex) {
            System.out.println("Error desconnecting from server" + ex.getMessage());
        }
    }

    public boolean isConnected() {
        return server != null && !server.isClosed();
    }

    public DataInputStream getDis() {
        return dis;
    }

    public DataOutputStream getDos() {
        return dos;
    }

    public void startListening() {
        th = new Thread(() -> {

            running = true;
            while (running) {
                handleAction();
            }
        });
        th.setDaemon(true);
        th.start();
    }

    private void handleAction() {

        try {
            int action = 0;
            String json = dis.readUTF();
            JsonObject jsonObj = Json.createReader(new StringReader(json)).readObject();
            action = jsonObj.getInt("action");

            switch (action) {

                case ACTION_SIGN_UP: {
                    // TODO Sign Up 
                    break;
                }

                case ACTION_LOGIN: {
                    // TODO Login 
                    break;
                }

                case ACTION_ONLINE_USERS: {
                    // TODO Online Users 
                    break;
                }
                case ACTION_SEND_REQUEST: {
                    String opponentName = jsonObj.getString("player1");
                    String score = jsonObj.getString("score");
                    Platform.runLater(() -> new IncomingRequestDialog().showRequestDialog(
                            new Stage(), opponentName, score)
                    );
                    break;
                }

                case ACTION_SEND_MOVE: {
                    // TODO Send Move 
                    break;
                }

                case ACTION_LOGOUT: {
                    // TODO Logout
                    break;
                }

            }

        } catch (IOException ex) {
            System.err.println("couldn't read from json: " + ex.getMessage());
        }
    }
}