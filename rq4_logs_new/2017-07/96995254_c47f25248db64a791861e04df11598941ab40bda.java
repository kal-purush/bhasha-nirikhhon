package net.internetmemory.genreclassification;

import SparkWARC.WARC;
import ml.dmlc.xgboost4j.java.XGBoostError;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jie on 11/07/17.
 */
public class ClassificationWarc {
    private static SparkConf conf = new SparkConf()
            .setAppName("WebsiteCategorizor")
            .setMaster("local[*]")
            .set("spark.driver.maxResultSize", "5g")
            .set("spark.executor.memory", "15g");
    private static JavaSparkContext sc = new JavaSparkContext(conf);
    private static Pattern pld = Pattern.compile("^([^/]+//[^/]+)($|/)");
    private static List<String> order = Arrays.asList("Forum", "News", "Blogs","Marketplace", "Spam", "Porn");

    static Map<String, List<Integer>> classify(String warcDir, String modelPath) throws XGBoostError, IOException, ClassNotFoundException {
        List<Map<String, List<Integer>>> resultMapList = new ArrayList<>();
        File[] warcFiles = new File(warcDir).listFiles();
        ClassificationWekaModel model = new ClassificationWekaModel();
        model.loadModel(modelPath);
        for(File file: warcFiles){
            Dataset<Row> warc = WARC.load(sc.sc(), file.getPath(), true, 0);
            List<ParseWarc.Webpage> webpages = ParseWarc.parseWarc(warc.toLocalIterator());
            JavaRDD<ParseWarc.Webpage> webpagesRDD = sc.parallelize(webpages)
                    .filter(webpage -> webpage.getStatus().equals("200"));
            Map<String, List<Integer>> resultMap = webpagesRDD.mapToPair(webpage -> {
                String p = getPLD(webpage.getUrl());
                String result = "";
                try {
                    result = model.predict(webpage.getContent(), webpage.getUrl());
                }catch (Exception e){
                    return new Tuple2<>(p, Arrays.asList(0,0,0,0,0,0));
                }
                int resultIdx = order.indexOf(result);
                List<Integer> temp = Arrays.asList(0,0,0,0,0,0);
                temp.set(resultIdx, 1);
                return new Tuple2<>(p, temp);
            }).reduceByKey((list1, list2) -> {
                List<Integer> list = new ArrayList<>();
                for(int i=0; i<list1.size(); i++){
                    list.add(list1.get(i) + list2.get(i));
                }
                return list;
            }).collectAsMap();
            resultMapList.add(resultMap);
        }
        return mergeMap(resultMapList);
    }

    static String getPLD(String url) throws MalformedURLException {
        Matcher m = pld.matcher(url);
        String p = null;
        if(m.find()){
            p = m.group(1);
        }
        return new URL(p).getHost();
    }

    static Map<String, List<Integer>> mergeMap(List<Map<String, List<Integer>>> mapList){
        Map<String, List<Integer>> merged = new Hashtable<>();
        for(Map<String, List<Integer>> map: mapList){
            for(Map.Entry<String, List<Integer>> entry: map.entrySet()){
                if(merged.containsKey(entry.getKey())){
                    List<Integer> temp = new ArrayList<>();
                    for(int i=0; i<entry.getValue().size(); i++){
                        temp.add(entry.getValue().get(i) + merged.get(entry.getKey()).get(i));
                    }
                    merged.put(entry.getKey(), temp);
                }else{
                    merged.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return merged;
    }

    public static void main(String[] args) throws XGBoostError, IOException, ClassNotFoundException {
        Map<String, List<Integer>> result =
                classify("/home/jie/warcs", "/home/jie/projects/GenreClassification/model/model.bin");
        List<Triple<String, List<Integer>, String>> toDump = new ArrayList<>();
        result.forEach((k, v) -> {
            int max = Collections.max(v);
            int pos = v.indexOf(max);
            String cls = order.get(pos);
            toDump.add(Triple.of(k, v, cls));
        });
        toDump.sort(Comparator.comparingInt(t -> Collections.max(t.getMiddle())));
        FileUtils.writeLines(new File("/home/jie/Desktop/ClassificationRes8.txt"), toDump);
    }
}