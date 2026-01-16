package de.container42.photos;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import java.io.*;
import java.util.stream.IntStream;

/**
 * Created by edi on 4/22/16.
 */
@WebServlet("/DisplayAlbumServlet")
@MultipartConfig()
public class DisplayAlbumServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        handleRequest(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        handleRequest(req, resp);
    }

    protected void handleRequest(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        ServletContext servletContext = req.getServletContext();
        PhotoAlbum pa = PhotoAlbum.getPhotoAlbum(servletContext);
        if(req.getContentType() != null && req.getContentType().startsWith("multipart/form-data")) {
            this.uploadPhoto(req, pa);
        }
        resp.setContentType("text/html;charset=UTF-8");
        PrintWriter writer = resp.getWriter();
        try {
            writer.write("<html>");
            writer.write("<head>");
            writer.write("<title>Photo Viewer</title>");
            writer.write("</head>");
            writer.write("<body>");
            writer.write("<h3 align='center'>Photos</h3>");
            this.displayAlbum(pa, "", writer);
            writer.write("</body>");
            writer.write("</html>");
        } finally {
            writer.close();
        }
    }

    private void uploadPhoto(HttpServletRequest req, PhotoAlbum pa) throws ServletException, IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        String filename = null;
        for(Part part: req.getParts()) {
            this.copyBytes(part.getInputStream(), baos);
            filename = part.getSubmittedFileName();
        }
        if(!"".equals(filename)) {
            String photoName = filename.substring(0, filename.lastIndexOf("."));
            pa.addPhoto(photoName, baos.toByteArray());
        }
    }

    private void displayAlbum(PhotoAlbum pa, String label, final PrintWriter writer) {
        writer.write("<h3 align='center'>" + label + "</h3>");
        writer.write("<table align='center'>");
        IntStream.range(0, pa.getPhotoCount()).forEach(index -> {
            writer.write("<td>");
            writer.write("<a href='./DisplayPhotoServlet?photo=" + index + "'>");
            writer.write("<img src='./DisplayPhotoServlet?photo=" + index + "' alt='photo' height='120' width='150'>");
            writer.write("</a>");
            writer.write("</td>");
        });
        writer.write("<td bgcolor='#cccccc' width='120' height='120'>");
        writer.write("<form align='left' action='DisplayAlbumServlet' method='post' enctype='multipart/form-data'>");
        writer.write("<input value='Choose' name='myFile' type='file' accept='image/jpeg'><br>");
        writer.write("<input value='Upload' type='submit'><br>");
        writer.write("</form>");
        writer.write("</td>");
        writer.write("</tr>");

        writer.write("<tr>");
        IntStream.range(0, pa.getPhotoCount()).forEach(index -> {
            writer.write("<td align='center'>");
            writer.write(pa.getPhotoName(index));
            writer.write("</td>");
        });
        writer.write("</tr>");

        writer.write("<tr>");
        IntStream.range(0, pa.getPhotoCount()).forEach(index -> {
            writer.write("<td align='center'>");
            writer.write("<a href='RemovePhotoServlet?photo=" + index + "'>remove</a>");
            writer.write("</td>");
        });
        writer.write("</tr>");
        writer.write("</table>");
    }

    private void copyBytes(InputStream is, OutputStream os) throws IOException {
        int i;
        while((i=is.read()) != -1) {
            os.write(i);
        }
        is.close();
        os.close();
    }
}