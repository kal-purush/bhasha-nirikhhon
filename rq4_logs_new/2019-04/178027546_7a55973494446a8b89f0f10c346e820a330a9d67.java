package com.example.mi_b_wizard.Network;

import android.os.Handler;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class GroupOwnerSocketHandler extends Thread{

    ServerSocket socket = null;
    private final int THREAD_COUNT = 10;
    private Handler handler;

    public GroupOwnerSocketHandler(Handler handler) throws IOException{
        try {
            socket = new ServerSocket(8888);
            this.handler = handler;
            System.out.println("GO-Socket started");
        }catch (IOException e){
            e.printStackTrace();
            pool.shutdownNow();
            throw e;
        }
    }

    private final  ThreadPoolExecutor pool = new ThreadPoolExecutor(THREAD_COUNT,THREAD_COUNT,10,TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>());

    @Override
    public void run() {
        super.run();

        while (true){
            try {
                pool.execute(new Server(socket.accept(),handler));
                System.out.println("Starting Input/Output handler...");
            }catch (IOException e){
                try {
                    if(socket != null && !socket.isClosed()){
                        socket.close();
                    }
                }catch (IOException ee){
                    e.printStackTrace();
                }
                e.printStackTrace();
                pool.shutdownNow();
                break;
            }
        }
    }
}