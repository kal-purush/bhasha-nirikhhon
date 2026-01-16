package lesson3.history;

import lesson3.ClientHandler;

import java.io.*;

public class HistoryKeeper {
    private String message;
    private String fileName;
    private final ClientHandler clientHandler;

    /*public HistoryKeeper() {
    }*/

    public HistoryKeeper(String message, String username, ClientHandler clientHandler) {
        this.message = message;
        this.fileName = username + ".txt";
        this.clientHandler = clientHandler;
    }

    public String getMessage() {
        return message;
    }

    public String getFileName() {
        return fileName;
    }

    public void write() {
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(fileName, true))) {
            String userName = clientHandler.getUsername();
            String str = userName + ": " + message + "\r\n";
            byte[] array = str.getBytes();
            out.write(array);
            out.flush();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}