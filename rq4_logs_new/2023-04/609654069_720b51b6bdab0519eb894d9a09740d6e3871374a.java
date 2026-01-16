package lesson_10.tests;

import com.codeborne.pdftest.PDF;
import com.codeborne.xlstest.XLS;
import com.google.common.io.Files;
import com.opencsv.CSVReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class FileTest {

    private ClassLoader cl = FileTest.class.getClassLoader();

    @Test
    void zippedTest() throws Exception {
        try (InputStream is = cl.getResourceAsStream("lesson_10/myZipForLesson10.zip");
             ZipInputStream zis = new ZipInputStream(is)) {
            ZipEntry entry;

            while ((entry = zis.getNextEntry()) != null) {
                switch (Files.getFileExtension(entry.getName())) {
                    case "pdf" -> {
                        PDF pdf = new PDF(zis);
                        Assertions.assertEquals("SA",pdf.author);
                    }
                    case "xlsx" -> {
                        XLS xls = new XLS(zis);
                        Assertions.assertEquals(xls.excel.getSheetAt(0).getRow(3).getCell(2).getStringCellValue(), "Наименование регионального филиала");
                    }
                    case "csv" -> {
                        CSVReader reader = new CSVReader(new InputStreamReader(zis));
                        List<String[]> content = reader.readAll();
                        Assertions.assertArrayEquals(new String[]{"Жиренок", "Антон", "Александрович"}, content.get(1));
                    }
                }
            }
        }
    }
    @Test
    void jsonTest() throws Exception {


    }
}




    /**
     {
     "name": "Иван",
     "age": 37,
     "mother": {
     "name": "Ольга",
     "age": 58
     },
     "children": [
     "Маша",
     "Игорь",
     "Таня"
     ],
     "married": true,
     "dog": null
     }*/