package com.olehedza.reviews.util;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CsvFileReader implements FileReader {

    @Override
    public List<String> readFile(String path) {
        try {
            URI resource = Objects.requireNonNull(CsvFileReader.class
                    .getClassLoader().getResource(path)).toURI();
            @Cleanup Stream<String> stream = Files.lines(Paths.get(resource));
            return stream.collect(Collectors.toList());
        } catch (IOException | URISyntaxException | NullPointerException e) {
            throw new UndeclaredThrowableException(e, "Can't read file");
        }
    }
}