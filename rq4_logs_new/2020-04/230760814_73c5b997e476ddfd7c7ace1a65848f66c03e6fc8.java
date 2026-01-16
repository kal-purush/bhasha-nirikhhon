package top.elgong.NIO.ChatRoom;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.util.Set;

public class CharRoomServer {

    private static int PORT = 8000;

    public CharRoomServer(int port) {
        PORT = port;
    }

    /**
     *  启动聊天服务功能
     */
    public void start() throws IOException {

        // 1, 创建选择器
        Selector selector = Selector.open();

        // 2, 创建ServerSocketChannel，并绑定监听端口
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();

        serverSocketChannel.bind(new InetSocketAddress(PORT));

        // 3, 将Channel设置为非阻塞模式
        serverSocketChannel.configureBlocking(false);

        // 4, 将Channel注册到Selector上，监听连接事件
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

        // 5, 循环调用Selector的select方法，检测就绪情况
        while (true){

            // 获取准备好的通道数量
            int numOfChannelOk = selector.select();

            if (numOfChannelOk == 0) continue;

            // 6, 调用selectedKeys方法获取就绪channel集合
            Set<SelectionKey> selectionKeys = selector.selectedKeys();

            // 7, 判断就绪事件种类，调用业务处理方法

            for (SelectionKey selectionKey : selectionKeys){

                // 8, 根据业务需要决定是否再次注册监听事件，重复执行第三步操作
                 // 连接事件
                if (selectionKey.isAcceptable()) {
                    acceptHandler(serverSocketChannel, selector);
                }

                // 可读事件
                if (selectionKey.isReadable()) {
                    try{
                        readHandler(selectionKey, selector);
                    }catch (IOException e){
                        e.printStackTrace();
                    }

                }
                
                // 移除
                selectionKeys.remove(selectionKey);
            }
        }





    }

    private void readHandler(SelectionKey selectionKey, Selector selector) throws IOException {

        // 先获取就绪通道
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();

        // 分配缓冲区
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);

        // 读取 数据
        StringBuffer message = new StringBuffer(1024 * 10);
        while (socketChannel.read(byteBuffer) > 0){
            // 转换为读模式
            byteBuffer.flip();

            message.append(Charset.forName("UTF-8").decode(byteBuffer));
        }


         // 将channel再次注册到selector上，监听他的可读事件
        // 每注册一次，只监控一次
        socketChannel.register(selector, SelectionKey.OP_READ);

         // 将客户端发送的请求信息 广播给其他客户端

        if (message.length() > 0) {
            // 广播给其他客户端
            broadCast(selector, socketChannel, message.toString());
        }

    }

    private void broadCast(Selector selector, SocketChannel sendDataChannel, String message) {

        // 获取到所有已接入的客户端channel
        Set<SelectionKey> selectionKeySet = selector.keys();

        // 循环向所有channel广播信息

        for (SelectionKey selectionKey : selectionKeySet){
            SelectableChannel channel = selectionKey.channel();
            // 剔除发消息的客户端
            if (channel instanceof SocketChannel
                    && channel != sendDataChannel) {
                try {
                    // 非消息发送者
                    ((SocketChannel) channel).write(
                            Charset.forName("UTF-8").encode(message));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

    }

    /**
     *
     * @param serverSocketChannel
     * @param selector
     * @throws IOException
     */
    private void acceptHandler(ServerSocketChannel serverSocketChannel, Selector selector) throws IOException {

        //如果要是接入事件，创建socketChannel
        SocketChannel socketChannel = serverSocketChannel.accept();


         // 将socketChannel设置为非阻塞工作模式
        socketChannel.configureBlocking(false);


         // 将channel注册到selector上， 开始监听可读事件
        socketChannel.register(selector, SelectionKey.OP_READ);


        // 回复客户端提示信息
        socketChannel.write(Charset.forName("UTF-8")
                .encode("---加入聊天 ！！！"));
        
    }

    /**
     * 内部测试类
     */
    static class main {

        public static void main(String[] args) {

            try {
                new CharRoomServer(8000).start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}