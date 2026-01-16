package contrroler;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet(name = "Downloading", urlPatterns = {"/Downloading"})
public class Downloading extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        String applicationPath = req.getServletContext().getRealPath("");
        File newFile = new File(applicationPath + "//files//hello.png");
        
        resp.setHeader("Content-Disposition", "attachment; filename=filename.jpg");

        OutputStream outputStream = resp.getOutputStream();
        
        Files.copy(newFile.toPath(), outputStream);

    }

}