import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Scanner;

import java.util.Comparator;

public class G13HM1
{
    // It is important to mark this class as `static`.
    public static class LengthComparator implements Serializable, Comparator<Double>
    {
        public int compare(Double a, Double b)
        {
            if (a < b) return -1;
            else if (a > b) return 1;
            return 0;
        }

    }
    public static void main(String[] args) throws FileNotFoundException
    {
        if (args.length == 0)
        {
            throw new IllegalArgumentException("Expecting the file name on the command line");
        }

        // Read a list of numbers from the program options
        ArrayList<Double> lNumbers = new ArrayList<>();
        Scanner s =  new Scanner(new File(args[0]));
        while (s.hasNext())
        {
            lNumbers.add(Double.parseDouble(s.next()));
        }
        s.close();

        // Setup Spark
        SparkConf conf = new SparkConf(true)
                .setAppName("Preliminaries");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create a parallel collection
        JavaRDD<Double> dNumbers = sc.parallelize(lNumbers);
        //dNumbers.cache();
        //dNumbers.count();
        //long start = System.currentTimeMillis();

        /* Max value with reduce function. */
        double d_maxValue1 = dNumbers.reduce((x, y) ->
        {
            if (x > y) return x;
            else return y;
        });
        System.out.println("The max value using the reduce function is " + d_maxValue1);

        //long end = System.currentTimeMillis();
        //System.out.println(end-start);

        /* Max value with max function. */
        double d_maxValue2 = dNumbers.max(new LengthComparator());
        System.out.println("The max value using the max function is " + d_maxValue2);

        /* Normalized values. */
        JavaRDD<Double> dNormalized = dNumbers.map((x) -> x/d_maxValue1);

        /* Calculate the probability of a value being between 20% and 80% of the maximum value. */
        JavaRDD<Double> dReducedSet = dNormalized.filter((x) -> x>=0.2 && x<=0.8); /* Values in the desired range. */
        double d_ReducedSetCount = dReducedSet.count(); /* Number of values in the desired range. */
        double d_probability = d_ReducedSetCount / dNormalized.count(); /* Probability. */
        System.out.println("The probability of an element being withing 20% and 80% of the maximum value is "
                + d_probability);

        /*HW2 tests*/
        /*
        try
        {
            System.in.read();
        }
        catch(java.io.IOException e)
        {

        }
        */
    }
}
