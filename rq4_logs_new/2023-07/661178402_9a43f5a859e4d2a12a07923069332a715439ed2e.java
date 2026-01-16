package wsffs.springframework.boot.web.server;

import org.eclipse.jetty.server.Server;

public class JettyEmbeddedWebServer implements WebServer {

    private int port = -1;
    private final Server server;

    public JettyEmbeddedWebServer(int port) {
        if (port > 0) {
            this.port = port;
            this.server = new Server(port);
        } else {
            throw new RuntimeException("포트 번호 지정 안함");
        }
    }

    @Override
    public void start() throws WebServerException {
        try {
            server.start();
        } catch (Exception e) {
            throw new WebServerException("웹 서버를 실행하는 도중에 문제가 발생하였습니다.", e);
        }
    }

    @Override
    public void stop() throws WebServerException {
        try {
            server.stop();
        } catch (Exception e) {
            throw new WebServerException("웹 서버를 정지하는 도중에 문제가 발생하였습니다.", e);
        }
    }
}