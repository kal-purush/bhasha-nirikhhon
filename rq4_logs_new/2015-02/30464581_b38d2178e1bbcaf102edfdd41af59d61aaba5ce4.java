package com.sck2.files;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author newlight77@gmail.com
 */
public class FileToString {

    /**
     * Java < 5 : BufferedReader
     * Read the entire text file using a BufferedReader.
     * <p>
     *
     * @param file a text file
     * @return file context as string
     * @throws IOException
     */
    public static String usingBufferedReader(File file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        StringBuffer fileContent = new StringBuffer();
        String line = br.readLine();
        while (line != null) {
            fileContent.append(line);
            line = br.readLine();
        }
        br.close();
        return fileContent.toString();
    }

    /**
     * Java 5 : File Scanner
     * Read the entire text file using java 5 Scanner.
     * <p>
     *
     * @param file a text file
     * @return file context as string
     * @throws FileNotFoundException
     */
    public static String usingScanner(File file) throws FileNotFoundException {
        Scanner scanner = new Scanner(file);
        String fileContent = scanner.nextLine(); // only reads line
        scanner.close();
        return fileContent;
    }

    /**
     * Java 7 : nio.Files.readAllLines
     * Read the file line of a text file using java 5 Scanner.
     * <p>
     *
     * @param path the path to a the text file
     * @return file context as string
     * @throws IOException
     */
    public static String usingNioFilesReadAllLines(Path path) throws IOException {
        List<String> file = Files.readAllLines(path, StandardCharsets.UTF_8);
        String fileContent = file.get(0);
        return fileContent;
    }

    /**
     * Java 7 : nio.Files => Read All Bytes
     * Read the entire text file using java 7 Files.readAllBytes.
     * <p>
     *
     * @param path the path to a the text file
     * @return file context as string
     * @throws java.io.IOException
     */
    public static String usingNioFilesReadBytes(Path path) throws IOException {
        String fileContent = new String(Files.readAllBytes(path));
        return fileContent;
    }

    /**
     * Java 8 : Collectors.joining
     * Read the entire text file using Files.lines and java 8 Collectors.joining() which will join
     * all the strings together.
     * <p>
     *
     * @param path the path to a the text file
     * @return file context as string
     * @throws java.io.IOException
     */
    public static String usingNioFilesWithCollectors(Path path) throws IOException {
        String fileContent = Files.lines(path).collect(Collectors.joining());
        return fileContent;
    }

    /**
     * Java 8 : Collectors.joining with lambda expression
     * <p>
     *
     * @param path the path to a the text file
     * @return file context as string
     * @throws java.io.IOException
     */
    public static String usingNioFilesWithCollectorsAndLambdaWithinTryBlock(Path path)
        throws IOException {
        //When filteredLines is closed, it closes underlying stream as well as underlying file.
        try (Stream<String> filteredLines = Files.lines(path)
            //test if file is closed or not
            .onClose(() -> System.out.println("File closed"))) {
            return filteredLines.collect(Collectors.joining());
        }
    }

    /**
     * Java 8: Files.lines and filters with lambda expression
     * <p>
     *
     * @param path the path to a the text file
     * @return file context as string
     * @throws java.io.IOException
     */
    public static String usingNioFilesWithFilteringAndLambda(Path path) throws IOException {
        //When filteredLines is closed, it closes underlying stream as well as underlying file.
        Stream<String> filteredLines =
            Files.lines(path).filter(s -> s.contains("File to string example"));
        return filteredLines.findFirst().get();

    }

    /**
     * Google Guava Files.toString
     * <p>
     *
     * @param file a text file
     * @return file context as string
     * @throws java.io.IOException
     */
    public static String usingGuava(File file) throws IOException {
        String fileContents = com.google.common.io.Files.toString(file, StandardCharsets.UTF_8);
        return fileContents;
    }

    /**
     * Apache Commons IO FileUtils.readFileToString
     * <p>
     *
     * @param file a text file
     * @return file context as string
     * @throws java.io.IOException
     */
    public static String usingApacheCommonsIo(File file) throws IOException {
        String fileContents = org.apache.commons.io.FileUtils.readFileToString(file);
        return fileContents;
    }
}
