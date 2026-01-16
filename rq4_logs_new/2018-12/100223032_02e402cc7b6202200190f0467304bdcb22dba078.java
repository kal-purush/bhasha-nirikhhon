package bottle.tcps.p;

import bottle.ftc.tools.Log;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public class SendContentHandle extends Thread{
    /**
     * 需要发送到的缓冲区包数据
     */
    private final BlockingQueue<ByteBuffer> sendBufferQueue = new LinkedBlockingQueue();

    private final Session session;

    private volatile boolean isFlag = true;

    public SendContentHandle(Session session) {
        this.session = session;
        this.setName("buf-send-t"+this.getId());
        this.start();
    }


    public void putBuf(ByteBuffer buffer) {

        try {
            Log.i("放入"+buffer);
            sendBufferQueue.put(buffer);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private long send_sum;
    private int sendIndex = 0;
    @Override
    public void run() {
        while (isFlag){

        try {
            ByteBuffer buffer = sendBufferQueue.take();
            SocketImp  socketImp = session.getSocketImp();
            try {

                Future<Integer> future =  socketImp.getSocket().write(buffer); //发送消息到管道
                sendIndex = 0;
                while(true){
                    if (future.isDone()){
                        send_sum+=future.get();
//                        Log.i(Thread.currentThread() +" 总发送 : " + send_sum + " byte");
                        break;
                    }
                    sendIndex++;
                    if (sendIndex >= 3) Thread.sleep(50 * sendIndex);
                }
            } catch (Exception e) {
                //发送数据异常
                socketImp.getAction().error(session,null,e);
                socketImp.getAction().connectClosed(session);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        }
    }

    public void clear() {
        isFlag = false;
        sendBufferQueue.clear();
    }
}