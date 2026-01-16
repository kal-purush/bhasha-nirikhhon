package com.zakrzewski.intentionbook.utils;

import com.itextpdf.text.*;
import com.itextpdf.text.pdf.PdfPCell;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfWriter;
import com.zakrzewski.intentionbook.entities.BookOfIntentionModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.LocalDate;
import java.util.List;

@Component
public class GeneratePdfIntention {
    private static final Logger logger = LoggerFactory.getLogger(GeneratePdfIntention.class);

    public ByteArrayInputStream intentionReport(List<BookOfIntentionModel> intentionModelList, LocalDate dateOfMass){
        Document document = new Document();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        String showDateFormat = "Data: " + dateOfMass + "\n" + "Godz.";
        //TODO how to make this like a builder design pattern?
        try {
            PdfPTable table = new PdfPTable(2);
            table.setWidthPercentage(80);
            table.setWidths(new int[]{1, 3});

            Font headFont = FontFactory.getFont(FontFactory.TIMES_ROMAN);

            PdfPCell hCell;
            hCell = new PdfPCell(new Phrase(showDateFormat, headFont));
            hCell.setHorizontalAlignment(Element.ALIGN_CENTER);
            table.addCell(hCell);

            hCell = new PdfPCell(new Phrase("Intencja", headFont));
            hCell.setHorizontalAlignment(Element.ALIGN_CENTER);
            table.addCell(hCell);

            for (BookOfIntentionModel intentionModel : intentionModelList){
                PdfPCell dateCell;
                dateCell = new PdfPCell(new Phrase(intentionModel.getTimeOfMass().toString()));
                dateCell.setVerticalAlignment(Element.ALIGN_MIDDLE);
                dateCell.setHorizontalAlignment(Element.ALIGN_CENTER);
                table.addCell(dateCell);


                PdfPCell intentionCell;
                intentionCell = new PdfPCell(new Phrase(intentionModel.getDescriptionOfIntention()));
                intentionCell.setPaddingLeft(5);
                intentionCell.setVerticalAlignment(Element.ALIGN_MIDDLE);
                intentionCell.setHorizontalAlignment(Element.ALIGN_LEFT);
                table.addCell(intentionCell);

            }

            PdfWriter.getInstance(document, outputStream);
            document.open();
            document.add(table);

            document.close();

        } catch (DocumentException e) {
            logger.error("Error: {0}", e);
        }

        return new ByteArrayInputStream(outputStream.toByteArray());

    }
}