import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

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


    }

    public void improvedWordCount1(JavaRDD<String> partitionedDocsRDD, int i_partitions)
    {

    }

    public void improvedWordCount2a(JavaRDD<String> partitionedDocsRDD, int i_partitions)
    {

    }

    public void improvedWordCount2b(JavaRDD<String> partitionedDocsRDD, int i_partitions)
    {

    }
}


