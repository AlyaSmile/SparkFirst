package com.virtualpairprogrammers;

import org.apache.spark.Partition;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Main {

    public static void main(String[] args) {
        //rddSamples();
        //pairsSamplesUdemy();
        //pairsSamples();

        Joins.TestJoins();
    }

    public  static void rddSamples()
    {
        List<Double> inputData = Arrays.asList(1.0, 25.0, 36.0, 625.0, 144.0, 100.0, 64.0, 16.0);
        System.out.println(inputData);

        SparkSession spark = SparkSession
                .builder()
                .appName("StartingSpark")
                .master("local[*]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("OFF");

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<Double> myRdd = sc.parallelize(inputData,3);

        Double result = myRdd.reduce(Double::sum);
        System.out.println("Reduce result:" +result);

        JavaRDD<Double> sqrtRdd = myRdd.map(Math::sqrt);
        sqrtRdd.foreach(x->System.out.println(x));
        System.out.println("Count:" + sqrtRdd.count());

        List<Double> outputData = sqrtRdd.collect();
        System.out.println("Collected data:" + outputData);

        JavaRDD<List<Double>> glom = myRdd.glom();
        System.out.println("Glom:" + glom.collect());


        JavaRDD<List<Double>> glomSqrt = sqrtRdd.glom();
        System.out.println("Glom sqrt:" + glomSqrt.collect());


        List<Partition> ggg = myRdd.partitions();

        sc.close();
    }

    public  static void pairsSamplesUdemy()
    {
        SparkSession spark = SparkSession
                .builder()
                .appName("Pairs-Basic")
                .master("local[4]")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        List<String> inputData = Arrays.asList("WARN:1 oct",
                "WARN:31 nov",
                "ERR:31 nov",
                "INFO:1 nov",
                "INFO:2 nov",
                "FATAL:2 nov");

        JavaRDD<String> originMsg = sc.parallelize(inputData,3);

        JavaPairRDD<String, String> pairRDD = originMsg.mapToPair(x->
        {
            String[] kv = x.split(":");
            return new Tuple2<>(kv[0], kv[1]);
        });

        // reduceByKey
        JavaPairRDD<String, String> reducedPairRDD =
                pairRDD.reduceByKey((value1, value2) -> value1 + ", " + value2);
        System.out.println("Reduced by key result: ");
        reducedPairRDD.foreach(tuple->System.out.println(tuple._1() + tuple._2()));

        // reduceByKey shortened

       /* System.out.println("Reduced by key shorted result: ");
        sc.parallelize(inputData,3)
                .mapToPair(x->
                {
                    String[] kv = x.split(":");
                    return new Tuple2<>(kv[0], kv[1]);
                })
                .reduceByKey((value1, value2) -> value1 + ", " + value2)
                .foreach(tuple->System.out.println(tuple._1() + tuple._2()));*/

        // CountByKey
        Map<String, Long> pairRddCount = pairRDD.countByKey();
        System.out.println("Count by key result: "+pairRddCount);

        // FlatMap
        JavaRDD<String> ff = pairRDD.flatMap(value ->
                Arrays.asList(value._2().split(" ")).iterator());
        System.out.println("flatMap: ");
        ff.foreach(x-> System.out.println(x));

        // filter
        JavaRDD<String> ff1 = ff.filter(x-> x.equals("1"));
        System.out.println("filter: ");
        ff1.foreach(x-> System.out.println(x));

        // foldByKey
        JavaPairRDD<String, String> foldedRDD =
                pairRDD.foldByKey("folded", (x, y) -> x +", "+ y);
        System.out.println("FoldByKey: ");
        foldedRDD.foreach(x-> System.out.println(x));

        //pairRDD.coalesce(1);
        //ff.coalesce(1);


        sc.close();
    }

    public  static void pairsSamples()
    {
        SparkSession spark = SparkSession
                .builder()
                .appName("Pairs-Basic")
                .master("local[4]")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        List<Tuple2<String, Integer>> pairs =
                Arrays.asList(
                        new Tuple2<>("1",9),
                        new Tuple2<>("1",2),
                        new Tuple2<>("1",1),
                        new Tuple2<>("2",3),
                        new Tuple2<>("2",4),
                        new Tuple2<>("3",1),
                        new Tuple2<>("3",5),
                        new Tuple2<>("6",2),
                        new Tuple2<>("6",1),
                        new Tuple2<>("6",4),
                        new Tuple2<>("8",1));

        // a randomly partitioned pair RDD
        JavaPairRDD<String, Integer> pairsRDD = sc.parallelizePairs(pairs, 4);

        System.out.println("*** the original pairs");
        pairsRDD.foreach(i -> System.out.println(i));

        //
        // Pairs can be collected as a Map of, but this only works well if the
        // keys are unique. Here they aren't so an arbitrary value is chosen for each:
        //
        Map<String, Integer> pairsAsMap = pairsRDD.collectAsMap();
        System.out.println("*** the pretty useless map");
        System.out.println(pairsAsMap);

        // let's say we just want the pair with minimum value for each key
        // we can use one of the handy methods in PairRDDFunctions. To reduce we need
        // only supply a single function to combine all the values for each key -- the result
        // has to have the same type as the values
        JavaPairRDD<String, Integer> reducedRDD = pairsRDD.reduceByKey(Math::min);

        System.out.println("*** the reduced pairs");
        reducedRDD.foreach(i -> System.out.println(i));

        // the reduced pairs have unique keys so collecting to a map works a lot better
        Map<String, Integer> reducedAsMap = reducedRDD.collectAsMap();
        System.out.println("*** the reduced pairs as a map");
        System.out.println(reducedAsMap);

        // folding is a little mor general: we get to specifiy the identity value:
        // say 0 for adding and 1 for multiplying
        JavaPairRDD<String, Integer> foldedRDD =
                pairsRDD.foldByKey(1, (x, y) -> x * y);

        System.out.println("*** the folded pairs");
        foldedRDD.foreach(i -> System.out.println(i));

        // Combining is more general: you can produce values of a different type, which is very powerful.
        // You need to provide three functions: the first converts an individual value to the new type, the second
        // incorporates an additional value into the the result, and the third combines intermediate results, which is
        // used by execution to avoid excessive communication between partitions. The first function is applied once
        // per partition and the second is used for each additional value in the partition.
        // Below is a pretty classical example of its use: compute a per-key average by first computing the sum and count
        // for each key and then dividing.
        JavaPairRDD<String, Tuple2<Integer, Integer>> combinedRDD =
                pairsRDD.combineByKey(
                        value -> new Tuple2<>(value, 1),
                        (sumAndCount, value) -> new Tuple2<>(sumAndCount._1() + value, sumAndCount._2() + 1),
                        (sumAndCount1, sumAndCount2) ->
                                new Tuple2<>(sumAndCount1._1() + sumAndCount2._1(), sumAndCount1._2() + sumAndCount2._2())
                );

        JavaPairRDD<String, Double> averageRDD =
                combinedRDD.mapValues(sumAndCount -> (double) sumAndCount._1() / sumAndCount._2());

        System.out.println("*** the average pairs");
        averageRDD.foreach(i -> System.out.println(i));

        // The dividing could be done just by calling map, but in Java this requires a lot of conversion between the
        // two kinds of RDD and ends up *VERY* cumbersome.
        JavaRDD<Tuple2<String, Tuple2<Integer, Integer>>> tupleCombinedRDD =
                JavaRDD.fromRDD(combinedRDD.rdd(), combinedRDD.classTag());
        JavaRDD<Tuple2<String, Double>> tupleDividedRDD = tupleCombinedRDD.map(keyAndsumAndCount ->
                new Tuple2<>(keyAndsumAndCount._1(), (double) keyAndsumAndCount._2()._1() / keyAndsumAndCount._2()._2()));
        JavaPairRDD<String, Double> averageRDDtheHardWay = JavaPairRDD.fromJavaRDD(tupleDividedRDD);

        // remember these won't necessarily come out int he same order so they may not obviously be
        // the same as above
        System.out.println("*** the average pairs the hard way");
        averageRDDtheHardWay.foreach(i -> System.out.println(i));

        spark.stop();
    }
}
