package com.lg.music.analyze.app.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.*;

/**
 * <p>
 * description:
 * </p>
 * Created on 2018/5/8 18:33
 *
 * @author leiguang
 */
public class WordCount {

    public static void main(String[] args){

        SparkConf sparkConf = new SparkConf().setAppName("musicDemoApp");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("file:///software_install/spark-2.2.1-bin-hadoop2.7/NOTICE");
        JavaPairRDD<String, Integer> wordsPair = stringJavaRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(Iterator<String> iterator) throws Exception {
                List<Tuple2<String, Integer>> result = new ArrayList<>();
                while (iterator.hasNext()) {
                    String s = iterator.next();
                    if (s.length() >0){
                        StringTokenizer st = new StringTokenizer(s);
                        while (st.hasMoreTokens()) {
                            result.add(new Tuple2<>(st.nextToken().toLowerCase(), 1));
                        }
                    }
                }
                return result.iterator();
            }
        });

        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = wordsPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer id, Integer id2) throws Exception {
                return id + id2;
            }
        });

        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD1 = stringIntegerJavaPairRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Integer>>, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {
                Set<Tuple2<String, Integer>> result = new TreeSet<>(new Comparator<Tuple2<String, Integer>>() {
                    @Override
                    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                        return o2._2 - o1._2;
                    }
                });
                while (tuple2Iterator.hasNext()) {
                    result.add(tuple2Iterator.next());
                }
                return result.iterator();
            }
        });
        stringIntegerJavaPairRDD1.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(String.format("%s -- %d", t._1(), t._2()));
            }
        });
    }

}
