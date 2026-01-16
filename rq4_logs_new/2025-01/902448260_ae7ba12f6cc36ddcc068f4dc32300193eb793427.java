package com.cgr.base.application.GeneralRules;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cgr.base.infrastructure.persistence.entity.GeneralRules.GeneralRulesEntity;
import com.cgr.base.infrastructure.persistence.repository.GeneralRules.GeneralRulesRepository;

@Service
public class GeneralRulesExportService {
    @Autowired
    private GeneralRulesRepository generalRulesRepository;

    // Método para generar el archivo CSV en memoria
    public ByteArrayOutputStream generateCsvStream() throws IOException {
        List<GeneralRulesEntity> generalRulesData = generalRulesRepository.findAll();


        CSVFormat csvFormat = CSVFormat.DEFAULT
            .builder()
            .setQuoteMode(QuoteMode.ALL_NON_NULL)  // Entrecomilla todos los campos no nulos
            .setDelimiter(',')                      // Usa coma como delimitador
            .setQuote('"')                          // Usa comillas dobles para encerrar campos
            .setEscape('\\')                        // Usa backslash como carácter de escape
            .setNullString("")                      // Convierte null a string vacío
            .setHeader(getCsvHeader())              // Establece los headers
            .build();
            
        // Usar un ByteArrayOutputStream para generar el CSV en memoria
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             OutputStreamWriter writer = new OutputStreamWriter(byteArrayOutputStream);
             CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat.withHeader(getCsvHeader()))) {

            // Escribir los datos dinámicamente en el CSV
            for (GeneralRulesEntity entidad : generalRulesData) {
                csvPrinter.printRecord(getCsvRecord(entidad));
            }

            writer.flush();
            csvPrinter.flush(); // Asegura que todo se haya escrito en el archivo
            return byteArrayOutputStream;
        }
    }

    // Método para obtener los nombres de los campos de la entidad como encabezado
    private String[] getCsvHeader() {
        Field[] fields = GeneralRulesEntity.class.getDeclaredFields();
        String[] headers = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            headers[i] = fields[i].getName();
        }
        return headers;
    }

    // Método para obtener los valores de los campos de una entidad como registro
    // Método para obtener los valores de los campos de una entidad como registro
private List<String> getCsvRecord(GeneralRulesEntity entidad) {
    Field[] fields = GeneralRulesEntity.class.getDeclaredFields();
    List<String> record = new ArrayList<>();

    for (Field field : fields) {
        try {
            field.setAccessible(true); // Asegura que podemos acceder a campos privados
            Object value = field.get(entidad); // Obtener el valor del campo
            record.add(value != null ? value.toString() : ""); // Si el valor es null, agregamos una cadena vacía
        } catch (IllegalAccessException e) {
            // Manejar la excepción, por ejemplo, agregando un valor por defecto
            record.add("ERROR"); // En caso de error, agregamos "ERROR" como valor
        }
    }

    return record;
}

}