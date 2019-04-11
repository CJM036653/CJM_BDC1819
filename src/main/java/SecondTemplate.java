/*
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;


public class SecondTemplate
{
    public static void main(String[] args) throws FileNotFoundException
    {
        JavaPairRDD<String, Long> wordcountpairs = docs
                // Map phase
                .flatMapToPair((document) ->
                {
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens)
                    {
                        counts.put(token, 1L + counts.getOrDefault(token, 0L));
                    }
                    for (Map.Entry<String, Long> e : counts.entrySet())
                    {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })
                // Reduce phase
                .groupByKey()
                .mapValues((it) ->
                {
                    long sum = 0;
                    for (long c : it)
                    {
                        sum += c;
                    }
                    return sum;
                });
    }
}

        /* Create pairs (word, 1) *//*
        JavaPairRDD<String, Long> wordsRDD = documentsRDD.flatMapToPair((x) ->
        {
        String[] words = x.split(" ");
        ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
        for (String word : words)
        {
        pairs.add(new Tuple2<>(word, 1L));
        }
        return pairs.iterator();
        });

*/

/* Print all elements in an RDD. */
        /*
        for (Tuple2<String, Long> element : count2aRDD.collect())
        {
            System.out.println(element._1() + " " + element._2());
        }
        */
        /*
        try
        {
            System.in.read();
        }
        catch(java.io.IOException e)
        {

        }
        */