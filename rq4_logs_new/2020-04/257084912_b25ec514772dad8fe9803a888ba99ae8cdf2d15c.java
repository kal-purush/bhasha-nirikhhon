package com.naukma.mip.search;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.naukma.mip.search.file.FileProcessor;
import com.naukma.mip.search.file.TxtProcessor;
import com.naukma.mip.search.model.Dictionary;
import com.naukma.mip.search.model.invertedindex.InvertedIndexDictionary;

public class BuildInvertedIndex {

    private static final FileProcessor processor = new TxtProcessor();
    private static final DictionaryRepository dictionaryRepository = new DictionaryRepository();

    public static void main(String[] args) {
        long start = System.currentTimeMillis();

        List<String> files = getAllFilesFromFolder(FileUtils.BOOKS_FOLDER_PATH);
        Dictionary dictionary = createDictionary(files);

        dictionaryRepository.saveDictionary(dictionary);

        long end = System.currentTimeMillis();
        System.out.println("Processing took: " + (end - start) + " ms");
    }

    private static Dictionary createDictionary(List<String> files) {
        Dictionary dictionary = new InvertedIndexDictionary();
        files.forEach(file -> processor.processFile(dictionary, file));
        return dictionary;
    }

    private static List<String> getAllFilesFromFolder(String folderPath) {
        try (Stream<Path> walk = Files.walk(Paths.get(folderPath))) {
            List<String> result = walk.map(x -> x.toString())
                    .filter(f -> f.endsWith(".txt")).collect(Collectors.toList());
            return result;
        } catch (IOException e) {
            throw new RuntimeException("Error during searching files");
        }
    }
}