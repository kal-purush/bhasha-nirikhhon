package com.example;

import com.lowagie.text.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;
import org.thymeleaf.templatemode.TemplateMode;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;
import org.xhtmlrenderer.pdf.ITextRenderer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.*;

public class RegistrationLafApplication {
    private static final Logger LOG = LoggerFactory.getLogger(RegistrationLafApplication.class);
    private static final String Excel_FILE = "/Users/ank/Downloads/old_Downloads/loanrestructure/src/main/resources";

    public static void main(String[] args) throws IOException, DocumentException {

        RegistrationLafApplication thymeleaf2Pdf = new RegistrationLafApplication();
        thymeleaf2Pdf.parseThymeleaf();
    }

    private void parseThymeleaf() throws IOException, DocumentException {
        ClassLoaderTemplateResolver templateResolver = new ClassLoaderTemplateResolver();
        templateResolver.setSuffix(".html");
        templateResolver.setTemplateMode(TemplateMode.HTML);

        TemplateEngine templateEngine = new TemplateEngine();
        templateEngine.setTemplateResolver(templateResolver);

        Context context = new Context();

        try {
            Class.forName("org.relique.jdbc.csv.CsvDriver");
            Connection conn = DriverManager.getConnection("jdbc:relique:csv:" + Excel_FILE);

            Statement stmt = conn.createStatement();
            ResultSet resultSet = stmt.executeQuery("SELECT * FROM registration_cumulative");

            while (resultSet.next()) {
                String phone = resultSet.getString("Mobile Number");
                context.setVariable("name", resultSet.getString("Name"));
                context.setVariable("pan", resultSet.getString("Pan Number"));
                context.setVariable("mobile", phone);
                context.setVariable("email", resultSet.getString("Email Id"));
                context.setVariable("pincode", resultSet.getString("PINCODE"));
                context.setVariable("timestamp", resultSet.getString("ensign timestamp"));
                String html = templateEngine.process("templates/registration_cumulative", context);

                generatePdfFromHtml(html, phone);
            }
        } catch (SQLException | ClassNotFoundException e) {
            LOG.error("Unable to parse");
            e.printStackTrace();
        }
    }

    public void generatePdfFromHtml(String html, String newLoanId) throws IOException, DocumentException {
        String file = "src/main/resources/laf/registration_" + newLoanId + ".pdf";
        OutputStream outputStream = new FileOutputStream(file);

        ITextRenderer renderer = new ITextRenderer();
        renderer.setDocumentFromString(html);
        renderer.layout();
        renderer.createPDF(outputStream);

//        GoogleDriveService.uploadToDrive(file);
        outputStream.close();
    }

}