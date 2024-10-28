package com.virtualpairprogrammers;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Joins {

    public static void TestJoins()
    {
        List<Tuple2<Integer, Integer>> visitsRaw = Arrays.asList(new Tuple2<>(4,18),
                new Tuple2<>(6,4),
                new Tuple2<>(10,9));

        List<Tuple2<Integer, String>> usersRaw = Arrays.asList(new Tuple2<>(1,"John"),
                new Tuple2<>(2,"Bob"),
                new Tuple2<>(3,"Alan"),
                new Tuple2<>(4,"Doris"),
                new Tuple2<>(5,"Mary"),
                new Tuple2<>(6,"Raquel"));

        SparkSession spark = SparkSession
                .builder()
                .appName("Pairs-Basic")
                .master("local[4]")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        sc.setLogLevel("ERROR");

        JavaPairRDD<Integer, Integer> visitsRdd = sc.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> usersRdd = sc.parallelizePairs(usersRaw);

        JavaPairRDD<Integer, Tuple2<Integer, String>> visitsJoin = visitsRdd.join(usersRdd);
        visitsJoin.foreach(x-> System.out.println(x._2()._2()));

        System.out.println("LeftJoin:");
        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> visitsLeftJoin = visitsRdd.leftOuterJoin(usersRdd);
        visitsLeftJoin.foreach(x-> System.out.println(x._2()._2().orElse("--no  value--").toUpperCase()));

        System.out.println("Right Join:");
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> visitsRightJoin = visitsRdd.rightOuterJoin(usersRdd);
        visitsRightJoin.foreach(x-> System.out.println("user " + x._2()._2() + "("+ x._1() + ")" + "has " +
                x._2()._1().orElse(0)));
    }
}
