import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.*;
import java.lang.String;

public class G14HM2 {

    public static void main (String [] args)
    {
        if (args.length == 0)
        {
            throw new IllegalArgumentException("Expecting the file name on the command line");
        }

        SparkConf conf = new SparkConf(true).setAppName("Homework");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> docs = sc.textFile(args[0]);
        JavaRDD<String> partioned_docs = docs.repartition(Integer.parseInt(args[1]));
        docs.cache();
        docs.count();


        /*
        long start = System.currentTimeMillis();
        JavaPairRDD<String, Long> words_1 = wordCount(docs);
        System.out.println("numero parole" + words_1.count());
        //words_1.foreach((VoidFunction<Tuple2<String, Long>>) couple -> System.out.println(couple._1()+" : "+couple._2));
        long end = System.currentTimeMillis();

        System.out.println("Execution time of Improved Word Count 1:  "+(end-start)+"ms");
*/
        long start = System.currentTimeMillis();
        JavaPairRDD<String, Long> words_Iwc1 = improvedWordCount1(docs);
        System.out.println("numero parole" + words_Iwc1.count());
        long end = System.currentTimeMillis();

        System.out.println("Execution time of Improved Word Count 1:  "+(end-start)+"ms");

        start = System.currentTimeMillis();
        JavaPairRDD<String, Long> words_Iwc2_random = improvedWordCount2_random(docs,Integer.parseInt(args[1]));
        System.out.println("numero parole" + words_Iwc2_random.count());
        end = System.currentTimeMillis();

        System.out.println("Execution time of Improved Word Count 2 with random keys" + ":  "+(end-start)+"ms");

        start = System.currentTimeMillis();
        JavaPairRDD<String, Long> words_Iwc2_Spark = improvedWordCount2_Spark(partioned_docs);
        System.out.println("numero parole" + words_Iwc2_Spark.count());
        end = System.currentTimeMillis();

        System.out.println("Execution time of Improved Word Count 2 with Spark partition " + ":  "+(end-start)+"ms");

       // System.out.println("The average length of the distinct words is:  "+ average(words_1));
        System.out.println("The average length of the distinct words is:  "+ average(words_Iwc1));
        System.out.println("The average length of the distinct words is:  "+ average(words_Iwc2_random));
        System.out.println("The average length of the distinct words is:  "+ average(words_Iwc2_Spark));
    }

    public static JavaPairRDD<String, Long> wordCount(JavaRDD<String> docs)
    {
        JavaPairRDD<String, Long> wordcountpairs = docs
                // Map phase
                .flatMapToPair((document) -> {
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        counts.put(token, 1L + counts.getOrDefault(token, 0L));
                    }
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })
                // Reduce phase
                .groupByKey()
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }
                    return sum;
                });

        return wordcountpairs;
    }


    public static JavaPairRDD<String, Long> improvedWordCount1 (JavaRDD<String> docs)
    {
        JavaPairRDD<String, Long> wordcountpairs = docs
                // Map phase
                .flatMapToPair((document) -> {
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        counts.put(token, 1L + counts.getOrDefault(token, 0L));
                    }
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })
                // Reduce phase
                .reduceByKey((a, b) -> a + b);

        wordcountpairs.cache();
        wordcountpairs.count();
        return wordcountpairs;
    }

    public static double average (JavaPairRDD<String,Long> words)
    {
        JavaRDD<String> occurences = words.keys();
        int sum = occurences.map((x)-> x.length()).reduce((x,y)->x+y);

        return ((double) sum)/((double) occurences.count());
    }


    public static JavaPairRDD<String, Long> improvedWordCount2_random (JavaRDD<String> docs, int k)
    {
        JavaPairRDD<String,Long> partition = docs
                // **************     Round 1      ***********************
                // Map phase
                // transforms each document in a Iterable object containing a Tuple2 for each word (Key=token, value=#occurences)
                .flatMapToPair((document) -> {
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        counts.put(token, 1L + counts.getOrDefault(token, 0L));
                    }
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })
                //associates to each Tuple2 (pair) a random key in the range [0,k-1]
                .groupBy(x -> {
                    Random num = new Random();
                    long value = num.nextLong() % ( (long) k);

                    return value;
                })
                //Reduce phase
                .flatMapToPair((couples) -> {
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    Iterator<Tuple2<String, Long>> set = couples._2().iterator();

                    while(set.hasNext())
                    {
                        Tuple2<String,Long> x = set.next();
                        counts.put(x._1(), x._2() + counts.getOrDefault(x._1(), 0L));
                    }

                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })
                .reduceByKey((x,y)->x+y);

        partition.cache();
        partition.count();

        return partition;
    }

    public static JavaPairRDD<String, Long> improvedWordCount2_Spark (JavaRDD<String> docs)
    {
        JavaPairRDD<String,Long> partition = docs
                //Round1
                .flatMapToPair((document) -> {
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        counts.put(token, 1L + counts.getOrDefault(token, 0L));
                    }
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })

                .mapPartitionsToPair((couples) -> {
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();

                    while(couples.hasNext())
                    {
                        Tuple2<String,Long> x = couples.next();
                        counts.put(x._1(), x._2() + counts.getOrDefault(x._1(), 0L));
                    }

                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })

                .reduceByKey((x,y) -> x + y );

        partition.cache();
        partition.count();
        return partition;
    }

}
