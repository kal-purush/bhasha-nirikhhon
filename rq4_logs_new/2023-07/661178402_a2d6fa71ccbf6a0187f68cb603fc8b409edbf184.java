package wsffs;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class DispatcherServlet extends HttpServlet {

    @Override
    protected void service(
            HttpServletRequest req,
            HttpServletResponse resp)
            throws ServletException, IOException {
        System.out.println("Hi");
        final String html = """
                <!DOCTYPE html>
                <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <title>Hello Servlet</title>
                </head>
                <body>
                <h1>Hi</h1>
                </body>
                </html>""";
        final byte[] rawBytes = html.getBytes(StandardCharsets.UTF_8);
        resp.setContentLength(rawBytes.length);
        resp.setContentType("text/html;charset=utf-8");
        final OutputStream outputStream = resp.getOutputStream();
        outputStream.write(rawBytes);
        outputStream.flush();
    }
}