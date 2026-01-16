import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;


public class CommonListener implements Runnable {
    private static final int CONNECTION_SIZE = TobGlobals.numNodes * 2;
    public volatile boolean isRunning = true;
    private ServerSocket listenerSocket;

    private int connector;

    public CommonListener(final ServerSocket listenerSocket) {

        this.listenerSocket = listenerSocket;

    }

    @Override
    public void run() {

        System.out.println("Listener start");

        Socket connectionSocket = null;

        try {

            while ((TobGlobals.getSocketMapSize() + MutexGlobals.getSocketMapSize()) < CONNECTION_SIZE) {

                try {
                    connectionSocket = listenerSocket.accept();

                    ObjectInputStream ois = new ObjectInputStream(connectionSocket.getInputStream());
                    byte[] buff = new byte[8];
                    ois.read(buff, 0, 8);
                    byte[] idBytes = new byte[4];
                    System.arraycopy(buff, 0, idBytes, 0, 4);

                    byte[] typeBytes = new byte[4];
                    System.arraycopy(buff, 4, typeBytes, 0, 4);

                    ByteBuffer bytebuff = ByteBuffer.wrap(idBytes);
                    int nodeId = bytebuff.getInt();
                    bytebuff = ByteBuffer.wrap(typeBytes);
                    int connType = bytebuff.getInt();

                    connector = nodeId;
                    if(nodeId == TobGlobals.id) {
                        addInputStreamEntry(nodeId, ois, connType);
//                        TobGlobals.addInputStreamEntry(nodeId, ois);
                        continue;
                    }
                    if(connType == 0) {
                        System.out.println("Tob Connected : " + nodeId);
                    } else {
                        System.out.println("Mutex Connected : " + nodeId);
                    }

                    addSocketEntry(nodeId, connectionSocket, connType);
                    addInputStreamEntry(nodeId, ois, connType);
                    addOutputStreamEntry(nodeId, new ObjectOutputStream(connectionSocket.getOutputStream()), connType);
//                    TobGlobals.addSocketEntry(nodeId, connectionSocket);
//                    TobGlobals.addInputStreamEntry(nodeId, ois);
//                    TobGlobals.addOutputStreamEntry(nodeId, new ObjectOutputStream(connectionSocket.getOutputStream()));

                } catch (IOException e) {
                    TobGlobals.log(connector + " - Listener : " + e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            TobGlobals.log(connector + " - Listener : " + e.getMessage());
            e.printStackTrace();
        } finally {
            System.out.println("Finally TobListener");
        }
    }

    private void addOutputStreamEntry(int nodeId,
            ObjectOutputStream objectOutputStream, int connType) {
        if (connType == 0) {
            TobGlobals.addOutputStreamEntry(nodeId, objectOutputStream);
        } else {
            MutexGlobals.addOutputStreamEntry(nodeId, objectOutputStream);
        }
    }

    private void addSocketEntry(int nodeId, Socket connectionSocket,
            int connType) {
        if(connType == 0) {
            TobGlobals.addSocketEntry(nodeId, connectionSocket);
        } else {
            MutexGlobals.addSocketEntry(nodeId, connectionSocket);
        }
    }

    private void addInputStreamEntry(int nodeId, ObjectInputStream ois,
            int connType) {
        if(connType == 0) {
            TobGlobals.addInputStreamEntry(nodeId, ois);
        } else {
            MutexGlobals.addInputStreamEntry(nodeId, ois);
        }
    }

}