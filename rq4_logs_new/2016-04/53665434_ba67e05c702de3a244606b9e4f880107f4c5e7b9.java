package ir.visoft.accounting.util;

import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.pdf.PdfWriter;
import ir.visoft.accounting.entity.Bill;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author Amir
 */
public class PdfUtil {

    public static Document createPdf(String filename) throws DocumentException, IOException {
        Document document = new Document();
        PdfWriter.getInstance(document, new FileOutputStream(filename));
        document.open();
        return document;
    }


    public static void createBillPdf(Bill bill, Document document) throws DocumentException {
        Paragraph paragraph = new Paragraph("bill Id" + ":" + bill.getBillId());

        document.add(new Paragraph("Hello World!"));
        document.add(new Paragraph(paragraph));
        document.add(new Paragraph("user Id" + ":" + bill.getUserId()));
        document.add(new Paragraph("final amount" + ":" + bill.getFinalAmount()));

        document.close();
    }

}