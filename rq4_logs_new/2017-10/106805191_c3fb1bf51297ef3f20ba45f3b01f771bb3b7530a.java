package com.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author liuxun
 * @version V1.0
 * @Description: spack单词计数器
 * @date 2017/10/23
 */
public class WordCount {

    public static void main(String[] args) {
        String inputpath="";
        String outpath="";
        if (args != null && args.length >= 2) {
            inputpath = args[0];
            outpath=args[1];
        }
        //创建spark context
        SparkConf conf=new SparkConf().setAppName("wordCount");
        JavaSparkContext sc=new JavaSparkContext(conf);
        //输入分析的目录
        JavaRDD<String> input = sc.textFile(inputpath);
        //切分单词
        JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                List<String> lines = Arrays.asList(s.split(" "));
                return lines.iterator();
            }
        });
        // 转换为键值对并计数
        JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        //输入结果存储在hdfs上
        counts.saveAsTextFile(outpath);
    }
}