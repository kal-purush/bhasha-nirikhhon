package ru.sandem.java3.lesson8;

import ru.sandem.java3.lesson8.filters.ChairOnlyFilter;
import ru.sandem.java3.lesson8.filters.JavaOnlyFilter;
import ru.sandem.java3.lesson8.server.Server;

public class StartServer {
    public static final String SERVER_HOST = "localhost"; //127.0.0.1
    public static final int SERVER_PORT = 9933;
    public static final String DB_NAME = "chat.db";

    public static void main(String[] args) {
        Server server = new Server(SERVER_PORT, DB_NAME);
        //чтобы основной поток не "замирал" в этом месте

        new Thread(server::waitForClient).start();
        server.addFilter(new JavaOnlyFilter());
        server.addFilter(new ChairOnlyFilter());
        //и т.д. - по ходу программы можем добавлять новые фильтры
    }
}