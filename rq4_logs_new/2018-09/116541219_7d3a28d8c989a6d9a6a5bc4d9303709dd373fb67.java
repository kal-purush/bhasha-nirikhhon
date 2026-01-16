package examples.MiniWAS;

import java.net.ServerSocket;
import java.net.Socket;

public class Connector {
    private int port;

    public Connector(int port) {
        this.port = port;
    }

    public void run() {
        ServerSocket serverSocket = null;

        try {

            serverSocket = new ServerSocket(port);

            // index.html 을 불러올때
            // 여기에서 while 을 해주면 사진도 불러오고.
            // while 없으면 그냥 html 문서만 읽고 사진은 안불러온다.

//            while(true) {
//                Socket socket = serverSocket.accept();
//                SocketHandler handler = new SocketHandler(socket);
//                handler.start();
//
//            }


            // 1 번만 호출하면 이미지를 불러오지 못한다. 왤까???
//            for(int i=0 ; i < 2 ; i++) {
//                Socket socket = serverSocket.accept();
//                SocketHandler handler = new SocketHandler(socket);
//                handler.start();
//            }
            Socket socket = serverSocket.accept();
            SocketHandler handler = new SocketHandler(socket);
            handler.start();



        }catch(Exception ex) {
            ex.printStackTrace();
        }finally {
            try {
                serverSocket.close();
            } catch(Exception e) {}
        }

    }


}