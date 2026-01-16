package com.mridx.shareshit.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Util {

    private static String zeroTo255 = "(\\d{1,2}|(0|1)\\"
            + "d{2}|2[0-4]\\d|25[0-5])";
    private static String regex = zeroTo255 + "\\."
            + zeroTo255 + "\\."
            + zeroTo255 + "\\."
            + zeroTo255;

    private static Pattern p = Pattern.compile(regex);

    private static Util instance;
    private ServerSocket serverSocket;

    public static Util getInstance() {
        if (instance == null) {
            instance = new Util();
        }
        return instance;
    }

    public ServerSocket getServerSocket() throws IOException {
        if (serverSocket == null) {
            serverSocket = new ServerSocket(Utils.CONNECT_HOST_PORT);
        }
        return serverSocket;
    }

    public void stopServer() {
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean validateIp(String s) {
        Matcher m = p.matcher(s);
        return m.matches();
    }


}