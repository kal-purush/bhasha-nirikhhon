package com.appspot;

import com.google.appengine.api.datastore.Blob;
import com.googlecode.objectify.ObjectifyService;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

/**
 * Created by eljah32 on 12/22/2015.
 */
public class BlobServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        long filedate=Long.parseLong(req.getParameter("date"));
        fileFor(filedate, resp);
    }

    void fileFor(long date, HttpServletResponse res) {

        UploadedFile file = new UploadedFile(date);
        Blob fileBytes = file.getFile();

        // serve the first image
        res.setContentType("application/vnd.ms-excel");
        res.setHeader("Content-Disposition", "attachment; filename="+file.getName());
        try {
            res.getOutputStream().write(fileBytes.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}