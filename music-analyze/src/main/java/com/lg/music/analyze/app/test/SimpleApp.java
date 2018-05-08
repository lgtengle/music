package com.lg.music.analyze.app.test;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * <p>
 * description:
 * </p>
 * Created on 2018/5/6 12:21
 *
 * @author leiguang
 */
public class SimpleApp {

    public static void main(String[] args){
        String file = "file:///software_install/spark-2.2.1-bin-hadoop2.7/NOTICE";
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        Dataset<String> cache = spark.read().textFile(file).cache();

        long a = cache.filter(new FilterFunction<String>() {
            @Override
            public boolean call(String s) throws Exception {
                return s.contains("a");
            }
        }).count();
        long b = cache.filter(new FilterFunction<String>() {
            @Override
            public boolean call(String s) throws Exception {
                return s.contains("b");
            }
        }).count();
        System.out.println("Line with a:"+a + ", lines with b:"+b);
        spark.stop();
    }
}
