import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map;


public class G13HM2
{
    public static void main(String[] args)
    {
        int i_partitions = -1;
        /* Input check. */
        try
        {
            i_partitions = Integer.parseInt(args[0]);
        }
        catch(NumberFormatException e)
        {
            System.out.println("Insert an integer.");
            return;
        }

        /* Creation of Spark configuration and context. */
        SparkConf configuration = new SparkConf(true)
                        .setAppName("application name here");
        JavaSparkContext sc = new JavaSparkContext(configuration);

        /* Dataset input. */
        JavaRDD<String> documentsRDD = sc.textFile(args[1]);
        JavaRDD<String> partitionedDocsRDD = documentsRDD.repartition(i_partitions); /* Dataset partitioning. */

        documentsRDD.cache();
        documentsRDD.count();
        long startTime = System.currentTimeMillis();
        JavaPairRDD<String, Long> count1RDD = improvedWordCount1(documentsRDD);
        long endTime = System.currentTimeMillis();
        System.out.println("The improved Word Count 1 takes " + (endTime - startTime) + "ms");

        /* Print all elements in an RDD. */
        /*
        for (Tuple2<String, Long> element : count1RDD.collect())
        {
            System.out.println(element._1() + " " + element._2());
        }
        */
        try
        {
            System.in.read();
        }
        catch(java.io.IOException e)
        {

        }

    }

    public static JavaPairRDD<String,Long> improvedWordCount1(JavaRDD<String> documentsRDD)
    {
        JavaPairRDD<String, Long> docRDD = documentsRDD
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
        docRDD.cache();
        docRDD.count();
        return docRDD;
    }

    public static void improvedWordCount2a(JavaRDD<String> partitionedDocsRDD, int i_partitions)
    {

    }

    public static void improvedWordCount2b(JavaRDD<String> partitionedDocsRDD, int i_partitions)
    {

    }
}


