package ru.itmo.icompiler.common;

import org.junit.jupiter.params.provider.Arguments;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class Common {
    public static Stream<Arguments> getTestFiles(String testsPath) throws IOException {
        Path resourceDir = Paths.get(testsPath);
        return Files.list(resourceDir)
                .filter(Files::isRegularFile)
                .map(path -> {
                    try {
                        String content = Files.readString(path);
                        return Arguments.of(path.getFileName().toString(), content);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}