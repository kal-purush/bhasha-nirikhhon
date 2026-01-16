package net.spright.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class MutiThreadSearch {

    public static void main(String[] args) throws InterruptedException, IOException {
        final int searchThreadCount = 10;
        final int pageCount = 500;
        Map<String, Integer> resultMap = new HashMap<>();
        final String keyword = args[0];
        final FileSystem fs = getFileSystem();
        
        ExecutorService service = Executors.newFixedThreadPool(
            searchThreadCount 
        );
        BlockingQueue<Path> pageQueue = new ArrayBlockingQueue(pageCount);
        
        try {
            initPageQueue(fs, pageQueue);
        } catch (IOException ex) {
           System.out.println(ex);
        }
        
        for (int i = 0; i != searchThreadCount; ++i) {
            service.execute(new Seacher(fs, keyword, pageQueue, resultMap));
        }
    
        service.shutdown();
        service.awaitTermination(5, TimeUnit.SECONDS);
        outputSearchResult(resultMap);
        System.exit(0);
    }
    private static void waitSomething(int time) throws InterruptedException {
        TimeUnit.SECONDS.sleep(time);
    }
   
    private static class Seacher implements Runnable {

        private final BlockingQueue<Path> pageQueue;
        private final Map<String, Integer> resultMap;
        private final String keyword;
        private final FileSystem fs;
        
        public  Seacher(
            FileSystem fs,
            String keyword,
            BlockingQueue<Path> pageQueue, 
            Map<String, Integer> resultMap
        ) {
            this.fs = fs;
            this.keyword = keyword;
            this.pageQueue = pageQueue;
            this.resultMap = resultMap;
        }
        @Override
        public void run() {
            int score = 0;
            Path path;
            try {
                while ((path = pageQueue.take()) != null) {
                    
                    HtmlPage page = getHtmlPage(fs, path);
                    // content, calculate corelation
                    System.out.println("Search link: " + page.link);
                   
                   
                    resultMap.put(page.link, score);
                }       
            } catch (InterruptedException ex) {
                System.out.println(ex);
                Thread.currentThread().interrupt();
            } catch (IOException ex) {
                Logger.getLogger(MutiThreadSearch.class.getName()).log(Level.SEVERE, null, ex);
            }
            
        }
    }
    
    private static void initPageQueue(FileSystem fs, 
            BlockingQueue<Path> pageQueue) throws IOException {
        FileStatus[] status = fs.listStatus(new Path("hdfs://course/user/course"));
        for (FileStatus s : status) {
            try {
                //System.out.println(s.toString());
                pageQueue.put(s.getPath());
            } catch (InterruptedException ex) {
                Logger.getLogger(MutiThreadSearch.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    private static FileSystem getFileSystem() throws IOException {
       Configuration configuration = new Configuration();
       FileSystem fs = FileSystem.get(configuration);
       return fs;
    }
       
    private static HtmlPage getHtmlPage(FileSystem fs, Path path)
            throws IOException {     
        
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(
                fs.open(path)
                , Charset.forName("UTF-8")))) {
                 
            String link = reader.readLine();
            String title = reader.readLine();
            String line;
        
            StringBuilder content = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                content.append(line);
            }
               
            return new HtmlPage(link, title, content.toString());
        }
        
    }
    
    private static void outputSearchResult(Map<String, Integer> map) {
        List<Map.Entry<String, Integer>> list =
            new LinkedList<>( map.entrySet() );
        System.out.println("output:");
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare( Map.Entry<String, Integer> o1, 
                    Map.Entry<String, Integer> o2) {
                return (o1.getValue()).compareTo( o2.getValue() );
            }
        });
       Iterator it = list.iterator();
       while(it.hasNext()) {
          Map.Entry entry;  
          entry = (Map.Entry) it.next();
          
          System.out.println(entry.getKey().toString() + entry.getValue().toString());
         
        }               
    }      
  
    private static class HtmlPage {
        private final String link;
        private final String title;
        private final String content;
        public HtmlPage(
            String link,
            String title,
            String content
        ) {
            this.link = link;
            this.title = title;
            this.content = content;
        }
        public String getTitle() {
            return title;
        }
        public String getLink() {
            return link;
        }
        public String getContent() {
            return content;
        }
    }
}