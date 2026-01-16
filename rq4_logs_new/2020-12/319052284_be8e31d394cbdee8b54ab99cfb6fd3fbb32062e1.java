package ru.geekbrains.servlet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.*;
import javax.servlet.annotation.WebServlet;
import java.io.IOException;

@WebServlet(name = "BasicServlet", urlPatterns = "/basic_servlet")
public class BasicServlet implements Servlet {

    private static Logger logger = LoggerFactory.getLogger(BasicServlet.class);

    public transient ServletConfig config;

        // Метод вызывается контейнером после того как был создан класс сервлета
    @Override
    public void init(ServletConfig servletConfig) throws ServletException {

        // Сохраняем полученную от сервера конфигурацию
        this.config = config;

    }

    @Override
    public ServletConfig getServletConfig() {
        return config;
    }

         // Метод вызывается для каждого нового HTTP запроса к данному сервлету
    @Override
    public void service(ServletRequest servletRequest, ServletResponse servletResponse) throws ServletException, IOException {
        logger.info("New request");

        // получаем объект типа BufferedWriter и пишем в него ответ на пришедший запрос
        servletResponse.getWriter().println("<h1>Servlet content</h1>");
    }

    @Override
    public String getServletInfo() {
        return "BasicServlet";
    }

        // При завершении работы веб приложения, контейнер вызывает этот метод для всех сервлетов из этого приложения
    @Override
    public void destroy() {
        logger.info("Servlet {} destroyed", getServletInfo());

    }
}