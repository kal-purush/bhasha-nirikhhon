package cz.codeland.gunlicencetester;

import org.apache.commons.lang3.Validate;
import org.apache.pdfbox.cos.COSDocument;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GunLicencePDFTextExtractor implements PDFTextExtractor
{
  private static final Logger LOGGER = Logger.getLogger(GunLicencePDFTextExtractor.class.getName());

  @Override
  public String extract(InputStream inputStreamPdf) throws IOException
  {
    String result;
    try (PDDocument pdfInMemory = createPdfInMemory(inputStreamPdf)) {
      PDFTextStripper stripper = new PDFTextStripper();
      result = stripper.getText(pdfInMemory).trim();
    } catch (RuntimeException e) {
      LOGGER.log(Level.SEVERE, e.getMessage(), e);
      throw e;
    }

    return result;
  }

  public PDDocument createPdfInMemory(InputStream inputStreamPdf) throws IOException
  {
    Validate.notNull(inputStreamPdf);

    PDDocument pdfInMemory;

    PDFParser parser = new PDFParser(inputStreamPdf);
    parser.parse();
    COSDocument parsedPdf = parser.getDocument();

    pdfInMemory = new PDDocument(parsedPdf);

    return pdfInMemory;
  }

  public InputStream readResourceFile(String fileName)
  {
    return this.getClass().getResourceAsStream(fileName);
  }
}