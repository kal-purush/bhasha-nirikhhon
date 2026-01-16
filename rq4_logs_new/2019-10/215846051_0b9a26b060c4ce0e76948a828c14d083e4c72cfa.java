package web_indexing;

import java.io.IOException;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPInputStream;

class Posting {
    private Integer offset;
    private Integer size;
    private Integer count;

    Posting(Integer offset, Integer size, Integer count) {
        this.offset = offset;
        this.size = size;
        this.count = count;
    }

    Integer getOffset() {
        return offset;
    }

    Integer getSize() {
        return size;
    }

    Integer count() {
        return count;
    }

}

class PostingList {
    private List<Integer> docIds;
    private List<Integer> frequencies;
    private Integer index;

    PostingList() {
        docIds = new ArrayList();
        frequencies = new ArrayList();
        index = -1;
    }

    public String[] decodeVarByteCompression(String varByteEncoding) {
        String[] decodings = new String[varByteEncoding.length() / 8];
        int j = 0;
        for (int i = 0; i < decodings.length; i++) {
            decodings[i] = varByteEncoding.substring(j, j + 8);
            j += 8;
        }
        return decodings;
    }

    public Integer nextGEQ(Integer k) {
        if (index > -1) {
            for (int i = index; i < docIds.size(); i++) {
                if (docIds.get(i) >= k)
                    return docIds.get(i);
            }
        }
        return null;
    }

    public Integer getFreq() {
        return (index > -1) ? frequencies.get(index) : null;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    public void createPostings(String fileName, Integer offset, Integer size) {
        try {
            RandomAccessFile invertedIndexFile = new RandomAccessFile(fileName, "r");
            invertedIndexFile.seek(offset);
            byte[] byteArray = new byte[size];
            invertedIndexFile.read(byteArray);
            String varByteEncoding = new String(byteArray);
            System.out.println("Byte accessed =" + varByteEncoding);
            String[] bytes = decodeVarByteCompression(varByteEncoding);
            StringBuffer sb = new StringBuffer();
            ArrayList<Integer> varByteDecodingList = new ArrayList();
            for (String byteString : bytes) {
                sb.append(byteString.substring(1));
                if (byteString.charAt(0) == '1') {
                    varByteDecodingList.add(Integer.parseInt(sb.toString(), 2));
                    sb = new StringBuffer();
                }
            }
            int i = 0;
            int docIdsCount = 0;
            for (Integer no : varByteDecodingList) {
                i++;
                if (i <= varByteDecodingList.size() / 2) {
                    docIdsCount += no;
                    docIds.add(docIdsCount);
                } else {
                    frequencies.add(no);
                }
            }
            if (docIds.size() > 0) {
                index++;
            }
            System.out.println("docIds ====" + docIds);
            System.out.println("frequencies ====" + frequencies);
        } catch (IOException e) {
            System.out.println("Unable to read content from file");
        }
    }
}

class Query {
    private HashMap<String, Posting> lexiconMap;
    private HashMap<Integer, String> docIdToUrlMap;
    private String keywords;

    Query(String keywords) {
        lexiconMap = new HashMap();
        docIdToUrlMap = new HashMap();
        this.keywords = keywords;
    }

    public void buildLexicon(String fileName) {
        try {
            GZIPInputStream lexiconFile = new GZIPInputStream(new FileInputStream(fileName));
            BufferedReader br = new BufferedReader(new InputStreamReader(lexiconFile));
            String currentTerm = null;
            while ((currentTerm = br.readLine()) != null) {
                String[] lexiconValues = currentTerm.split(" ");
                System.out.println("currentTerm ======" + currentTerm);
                if (lexiconValues.length == 4) {
                    String term = lexiconValues[0];
                    Integer offset = Integer.parseInt(lexiconValues[1]);
                    Integer size = Integer.parseInt(lexiconValues[2]);
                    Integer count = Integer.parseInt(lexiconValues[3]);
                    lexiconMap.put(term, new Posting(offset, size, count));
                }
            }
            System.out.println("lexiconMapSize =====" + lexiconMap.size());
            lexiconFile.close();
        } catch (IOException e) {
            System.out.println("Unable to read content from file");
        }
    }

    public void buildDocIdsToUrlMapping(String fileName) {
        try {
            GZIPInputStream lexiconFile = new GZIPInputStream(new FileInputStream(fileName));
            BufferedReader br = new BufferedReader(new InputStreamReader(lexiconFile));
            String currentTerm = null;
            while ((currentTerm = br.readLine()) != null) {
                String[] docIdsToUrlMappingValues = currentTerm.split(" ");
                System.out.println("currentTerm ======" + currentTerm);
                if (docIdsToUrlMappingValues.length == 2) {
                    Integer docId = Integer.parseInt(docIdsToUrlMappingValues[0]);
                    String url = docIdsToUrlMappingValues[1];
                    docIdToUrlMap.put(docId, url);
                }
            }
            System.out.println("buildDocIdsToUrlMappingSize =====" + docIdToUrlMap.size());
            lexiconFile.close();
        } catch (IOException e) {
            System.out.println("Unable to read content from file");
        }
    }

    public static void main(String[] args) {
        Query query = new Query("aaaa and zzzz");
        // query.buildLexicon("./lexicon.gz");
        // query.buildDocIdsToUrlMapping("./url_doc_mapping");
        PostingList pl = new PostingList();
        pl.createPostings("./invertedIndex", 93553216, 5120);

    }
}