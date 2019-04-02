import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

public class G13HM2
{
    public static void main(String[] args)
    {
        /* Creation of Spark configuration and context. */
        SparkConf configuration = new SparkConf(true)
                        .setAppName("application name here");
        JavaSparkContext sc = new JavaSparkContext(configuration);

        /* Dataset input. */
        JavaRDD<String> documentsRDD = sc.textFile(args[0]);
    }
}
