package web_indexing;

import java.io.*;
import java.util.zip.*;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.apache.commons.io.IOUtils;
import org.archive.io.warc.WARCReaderFactory;
import java.util.HashMap;
import java.util.Map;

class GeneratePostings {
    private String wetFilesPath;
    private GZIPOutputStream urlToDocMappingFile;
    private File postingsDirectory;

    GeneratePostings(String wetFilesPath, String postingsOutputPath) {
        this.wetFilesPath = wetFilesPath;
        this.urlToDocMappingFile = createUrlToDocMapping();
        this.postingsDirectory = createOutputDirectory(postingsOutputPath);
    }

    private GZIPOutputStream createUrlToDocMapping() {
        try {
            return new GZIPOutputStream(new FileOutputStream("./url_doc_mapping.gz"));
        } catch (IOException e) {
            System.out.println("Unable to create URL to Doc Mapping file");
        }
        return null;
    }

    public Boolean ifDirectoryAndMappingDocumentCreated() {
        return postingsDirectory != null && urlToDocMappingFile != null;
    }

    private File createOutputDirectory(String postingsPath) {
        File file = new File(postingsPath);
        if (!file.exists()) {
            if (file.mkdir()) {
                System.out.println("Directory is created!");
            } else {
                System.out.println("Failed to create directory!");
            }
        }

        return file;
    }

    private Boolean isCorrectWord(String word) {
        return ((word != null) && (word.length() > 2) && (word.matches("^[a-zA-Z]*$")));
    }

    private Map<String, Integer> findWordsCount(String[] words) {
        Map<String, Integer> wordsCount = new HashMap<String, Integer>();
        for (String word : words) {
            word = word.toLowerCase();
            if (!isCorrectWord(word)) {
                continue;
            }
            Integer count = wordsCount.getOrDefault(word, 0);
            wordsCount.put(word, count + 1);
        }
        return wordsCount;
    }

    private Boolean isPageValid(Map<String, Integer> wordsCount, int totalWords) {
        int totalWordsAfterParsing = 0;
        for (Integer count : wordsCount.values()) {
            totalWordsAfterParsing += count;
        }

        return (float) totalWordsAfterParsing / totalWords > 0.1;
    }

    public void createPostings() {
        final File folder = new File(wetFilesPath);
        int fileEntryIndex = 0;
        long totalUrls = 0;
        try {
            for (final File fileEntry : folder.listFiles()) {
                fileEntryIndex++;
                FileInputStream is = new FileInputStream(fileEntry);
                ArchiveReader ar = WARCReaderFactory.get(wetFilesPath + "/" + fileEntry.getName(), is, true);
                GZIPOutputStream gzos = new GZIPOutputStream(
                        new FileOutputStream(postingsDirectory.getName() + "/" + fileEntryIndex + ".gz"));
                for (ArchiveRecord r : ar) {

                    ArchiveRecordHeader header = r.getHeader();
                    String url = r.getHeader().getUrl();
                    if (url == null) {
                        continue;
                    }
                    System.out.println("totalURLs =" + totalUrls + " URL=" + url);

                    byte[] rawData = IOUtils.toByteArray(r, r.available());

                    String content = new String(rawData);
                    String[] words = content.split(" ");

                    Map<String, Integer> wordsCount = findWordsCount(words);

                    if (!isPageValid(wordsCount, words.length)) {
                        continue;
                    }
                    totalUrls++;
                    for (String word : wordsCount.keySet()) {
                        String posting = word + " " + totalUrls + " " + wordsCount.get(word) + "\n";
                        gzos.write(posting.getBytes());
                    }

                    urlToDocMappingFile.write((totalUrls + " " + url + " \n").getBytes());

                    System.out.println("=-=-=-=-=-=-=-=-=");

                }
                gzos.finish();
                gzos.close();
            }
            urlToDocMappingFile.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        GeneratePostings gp = new GeneratePostings("./wet_files", "./postings");
        if (gp.ifDirectoryAndMappingDocumentCreated())
            gp.createPostings();
        System.out.println("Total time =" + (System.currentTimeMillis() - startTime) / 60000.0);
    }
}