package gt.edu.usac.cunoc.ingenieria.eps.config;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class PdfPreviewServlet extends HttpServlet {
private static final long serialVersionUID = 1L;

@Override
protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    byte[] pdfBytesArray = (byte[]) request.getSession().getAttribute("pdfBytesArray");
    request.getSession().removeAttribute("pdfBytesArray");
    response.setContentType("application/pdf");
    response.setContentLength(pdfBytesArray.length);
    response.getOutputStream().write(pdfBytesArray);
} }