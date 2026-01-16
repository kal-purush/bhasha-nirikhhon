import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;

public class CommandServer implements Runnable {
    private final VncViewer viewer;

    public CommandServer(VncViewer v) {
        this.viewer = v;
    }

    public void run() {
        try {

            HttpServer server = HttpServer.create(new InetSocketAddress(8010), 0);
            server.createContext("/keyevent", new OnKeyEvent(viewer));
            server.setExecutor(null);
            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}