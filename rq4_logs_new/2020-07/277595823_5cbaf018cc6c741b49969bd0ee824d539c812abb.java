package com.olehedza.reviews.util;

import com.olehedza.reviews.dto.parser.CsvDto;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
@Slf4j
public class CsvParser implements Parser<CsvDto> {
    private final FileReader fileReader;

    @Override
    public List<CsvDto> parse(String path) throws IOException {
        List<CsvDto> dtos = new ArrayList<>();
        StringBuilder stringBuilder = new StringBuilder();
        for (String string : fileReader.readFile(path)) {
            stringBuilder.append(string).append("\n");
        }
        Iterable<CSVRecord> records = CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .parse(new StringReader(stringBuilder.toString()));
        for (CSVRecord record : records) {
            if (record.size() == CsvHeaders.values().length) {
                CsvDto dto = CsvDto.builder()
                        .productId(record.get(CsvHeaders.ProductId))
                        .userId(record.get(CsvHeaders.UserId))
                        .profileName(record.get(CsvHeaders.ProfileName))
                        .helpfulnessNumerator(Integer.parseInt(record
                                .get(CsvHeaders.HelpfulnessNumerator)))
                        .helpfulnessDenominator(Integer.parseInt(record
                                .get(CsvHeaders.HelpfulnessDenominator)))
                        .score(Integer.parseInt(record.get(CsvHeaders.Score)))
                        .time(Long.parseLong(record.get(CsvHeaders.Time)))
                        .summary(record.get(CsvHeaders.Summary))
                        .text(record.get(CsvHeaders.Text))
                        .build();
                log.info(dto.toString());
                dtos.add(dto);
            }
        }
        return dtos;
    }

    enum CsvHeaders {
        Id, ProductId, UserId,
        ProfileName, HelpfulnessNumerator,
        HelpfulnessDenominator, Score,
        Time, Summary, Text
    }
}