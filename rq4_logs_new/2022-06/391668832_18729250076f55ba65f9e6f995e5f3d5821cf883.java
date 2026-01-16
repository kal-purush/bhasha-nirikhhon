package ro.fasttrackit.bookapp.utility;

import com.lowagie.text.*;
import com.lowagie.text.pdf.PdfWriter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ro.fasttrackit.bookapp.model.BookEntity;
import ro.fasttrackit.bookapp.model.BookFileEntity;
import ro.fasttrackit.bookapp.model.CoverEntity;
import ro.fasttrackit.bookapp.respository.CoverRepository;
import ro.fasttrackit.bookapp.service.BookFileService;
import ro.fasttrackit.bookapp.service.BookService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;

@Component
@RequiredArgsConstructor
public class PdfGenerator {

    private final BookFileService bookFileService;
    private final CoverRepository coverRepository;

    public BookFileEntity pdfGenerator(BookEntity request) {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            Document document = new Document(PageSize.A4);
            PdfWriter.getInstance(document, byteArrayOutputStream);
            Optional<CoverEntity> byId = coverRepository.findById(request.getCoverId());
            CoverEntity coverEntity = byId.get();
            Image jpg = Image.getInstance(coverEntity.getImage().getData());
            float documentWidth = document.getPageSize().getWidth() - document.leftMargin() - document.rightMargin();
            float documentHeight = document.getPageSize().getHeight() - document.topMargin() - document.bottomMargin();
            jpg.scaleToFit(documentWidth, documentHeight);

            document.open();
            Font fontTitle = FontFactory.getFont(FontFactory.HELVETICA_BOLD);
            fontTitle.setSize(18);

            Paragraph paragraph = new Paragraph(request.getTitle(), fontTitle);
            paragraph.setAlignment(Paragraph.ALIGN_CENTER);

            Font fontParagraph = FontFactory.getFont(FontFactory.HELVETICA);
            fontParagraph.setSize(12);

            Paragraph paragraph2 = new Paragraph(request.getBookText(), fontParagraph);
            paragraph2.setAlignment(Paragraph.ALIGN_LEFT);

            document.add(jpg);
            document.add(paragraph);
            document.add(paragraph2);
            document.close();
            return bookFileService.addBookFile(request.getTitle(), byteArrayOutputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}