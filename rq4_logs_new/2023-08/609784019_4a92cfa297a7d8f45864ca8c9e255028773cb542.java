package application;

import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.pdf.PdfWriter;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;

public class CreateEncryptedPdf {
    public static void create(String[] args) {
        try {
            Document document = new Document();

            // Specify file path, give it a name
            String userProfile = System.getenv("USERPROFILE");
            String filePath = userProfile + "\\Documents\\encryptedPdf.pdf";

            PdfWriter writer = PdfWriter.getInstance(document, new FileOutputStream(filePath));

            // Set passwords and permissions
            writer.setEncryption("userPassword".getBytes(), "ownerPassword".getBytes(),
                    PdfWriter.ALLOW_PRINTING, PdfWriter.ENCRYPTION_AES_128);

            document.open();

            // Add content to PDF
            document.add(new Paragraph("Hello, World!"));

            document.close();
            writer.close();
        } catch (DocumentException | FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
