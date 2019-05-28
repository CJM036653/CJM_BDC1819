import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.BLAS;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class G13HM4
{
    public static void main(String[] args) throws Exception
    {

        //------- PARSING CMD LINE ------------
        // Parameters are:
        // <path to file>, k, L and iter

        if (args.length != 4) {
            System.err.println("USAGE: <filepath> k L iter");
            System.exit(1);
        }
        String inputPath = args[0];
        int k=0, L=0, iter=0;
        try
        {
            k = Integer.parseInt(args[1]);
            L = Integer.parseInt(args[2]);
            iter = Integer.parseInt(args[3]);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        if(k<=2 && L<=1 && iter <= 0)
        {
            System.err.println("Something wrong here...!");
            System.exit(1);
        }
        //------------------------------------
        final int k_fin = k;

        //------- DISABLE LOG MESSAGES
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        //------- SETTING THE SPARK CONTEXT      
        SparkConf conf = new SparkConf(true).setAppName("kmedian new approach");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //------- PARSING INPUT FILE ------------
        JavaRDD<Vector> pointset = sc.textFile(args[0], L)
                .map(x-> strToVector(x))
                .repartition(L)
                .cache();
        long N = pointset.count();
        System.out.println("Number of points is : " + N);
        System.out.println("Number of clusters is : " + k);
        System.out.println("Number of parts is : " + L);
        System.out.println("Number of iterations is : " + iter);

        //------- SOLVING THE PROBLEM ------------
        double obj = MR_kmedian(pointset, k, L, iter);
        System.out.println("Objective function is : <" + obj + ">");
    }

    public static Double MR_kmedian(JavaRDD<Vector> pointset, int k, int L, int iter)
    {
        long startTime = 0;
        long endTime = 0;

        //------------- ROUND 1 ---------------------------
        startTime = System.currentTimeMillis();

        JavaRDD<Tuple2<Vector,Long>> coreset = pointset.mapPartitions(x ->
        {
            ArrayList<Vector> points = new ArrayList<>();
            ArrayList<Long> weights = new ArrayList<>();
            while (x.hasNext())
            {
                points.add(x.next());
                weights.add(1L);
            }
            ArrayList<Vector> centers = kmeansPP(points, weights, k, iter);
            ArrayList<Long> weight_centers = compute_weights(points, centers);
            ArrayList<Tuple2<Vector,Long>> c_w = new ArrayList<>();
            for(int i =0; i < centers.size(); ++i)
            {
                Tuple2<Vector, Long> entry = new Tuple2<>(centers.get(i), weight_centers.get(i));
                c_w.add(i,entry);
            }
            return c_w.iterator();
        });

        coreset.cache();
        coreset.count();
        endTime = System.currentTimeMillis();
        System.out.println("Round 1 of MR_kmedian requires " + (endTime - startTime) + " ms.");

        //------------- ROUND 2 ---------------------------
        startTime = System.currentTimeMillis();

        ArrayList<Tuple2<Vector, Long>> elems = new ArrayList<>(k*L);
        elems.addAll(coreset.collect());
        ArrayList<Vector> coresetPoints = new ArrayList<>();
        ArrayList<Long> weights = new ArrayList<>();
        for(int i =0; i< elems.size(); ++i)
        {
            coresetPoints.add(i, elems.get(i)._1);
            weights.add(i, elems.get(i)._2);
        }

        ArrayList<Vector> centers = kmeansPP(coresetPoints, weights, k, iter);

        endTime = System.currentTimeMillis();
        System.out.println("Round 2 of MR_kmedian requires " + (endTime - startTime) + " ms.");

        //------------- ROUND 3: COMPUTE OBJ FUNCTION -------------------
        startTime = System.currentTimeMillis();

        double obj = pointset.map(x ->
            {
                double distance = Math.sqrt(Vectors.sqdist(centers.get(0), x));
                for (int i = 1; i < centers.size(); i++)
                {
                    double newDistance = Math.sqrt(Vectors.sqdist(centers.get(i), x));
                    if (newDistance < distance)
                    {
                        distance = newDistance;
                    }
                }
                return distance;
            })
            .reduce((x,y) -> x+y);

        endTime = System.currentTimeMillis();
        System.out.println("Round 3 of MR_kmedian requires " + (endTime - startTime) + " ms.");

        return obj/pointset.count();
    }

    public static ArrayList<Long> compute_weights(ArrayList<Vector> points, ArrayList<Vector> centers)
    {
        Long weights[] = new Long[centers.size()];
        Arrays.fill(weights, 0L);
        for(int i =0; i < points.size(); ++i)
        {
            double tmp = euclidean(points.get(i), centers.get(0));
            int mycenter = 0;
            for(int j = 1; j < centers.size(); ++j)
            {
                if(euclidean(points.get(i),centers.get(j)) < tmp)
                {
                    mycenter = j;
                    tmp = euclidean(points.get(i), centers.get(j));
                }
            }
            weights[mycenter] += 1L;
        }
        ArrayList<Long> fin_weights = new ArrayList<>(Arrays.asList(weights));
        return fin_weights;
    }

    public static Vector strToVector(String str) {
        String[] tokens = str.split(" ");
        double[] data = new double[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return Vectors.dense(data);
    }

    // Euclidean distance
    public static double euclidean(Vector a, Vector b) {
        return Math.sqrt(Vectors.sqdist(a, b));
    }


    /* KmeansPP from Homework 3. */
    public static ArrayList<Vector> kmeansPP(ArrayList<Vector> P, ArrayList<Long> WP, int k, int iter)
    {
        /* Internal class to store the distance of each point of P to C and the index of the corresponding centre. */
        class Pair
        {
            Pair(double d, int i)
            {
                distance = d;
                index = i;
            }
            double distance;
            int index;
        }

        /* Set of centres. */
        ArrayList<Vector> C = new ArrayList<>(k);

        /********************************** KMEANS++ **********************************/
        /* Add first random element with uniform distribution. */
        Random randomGenerator = new Random();
        int randomInt = randomGenerator.nextInt(P.size());
        C.add(P.get(randomInt));

        /* Store the minimum distance of all points in P from C. */
        ArrayList<Pair> currentDistances = new ArrayList<>(P.size());
        /* Compute the minimum distance of all points in P from the first centre. */
        for (int j = 0; j < P.size(); j++)
        {
            currentDistances.add(new Pair(Math.sqrt(Vectors.sqdist(P.get(j), C.get(0))), 0));
        }

        for (int i = 1; i < k; i++)
        {
            /* Compute the weighted sum of distances of points in P from C. */
            double sumOfDistances = 0;
            for (int j = 0; j < currentDistances.size(); j++)
            {
                sumOfDistances += WP.get(j) * currentDistances.get(j).distance;
            }
            /* Choose a random value between 0 and 1. */
            double chosenValue = randomGenerator.nextDouble();
            double probabilitySum = 0;
            int j = 0;
            /* Find the next centre, chosen with the appropriate probability distribution. */
            boolean done = false;
            while (!done)
            {
                double currentProbability = (WP.get(j) * currentDistances.get(j).distance)/sumOfDistances;
                double newProbabilitySum = probabilitySum + currentProbability;
                /* If the current element is not already a centre... */
                if (currentProbability != 0)
                {
                    /* If chosenValue is between probabilitySum and newProbabilitySum add the corresponding point to C and end the loop. */
                    if (newProbabilitySum >= chosenValue)
                    {
                        C.add(P.get(j));
                        done = true;
                    }
                }
                probabilitySum = newProbabilitySum;
                j++;
            }

            /* Recompute the minimum distance of points in P from C. */
            for (j = 0; j < P.size(); j++)
            {
                double distance = Math.sqrt(Vectors.sqdist(P.get(j), C.get(i)));
                if (distance < currentDistances.get(j).distance) currentDistances.set(j, new Pair(distance, i));
            }
        }

        /********************************** LLOYD **********************************/
        /* Perform iter iterations of LLoyd's algorithm. */
        for (int i = 0; i < iter; i++)
        {
            /* ArrayList containing currently computed clusters as indexes of the points contained in P. */
            ArrayList<ArrayList<Integer>> clusters = new ArrayList<>(k);

            /* Partition(P,C) */
            /* Create the clusters. */
            for (int z = 0; z < k; z++)
            {
                clusters.add(new ArrayList<>());
            }
            /* Add each point to its cluster. */
            for (int z = 0; z < P.size(); z++)
            {
                clusters.get(currentDistances.get(z).index).add(z);
            }

            /* Compute new centroids. */
            for (int z = 0; z < k; z++)
            {
                Vector c = Vectors.zeros(P.get(0).size());
                long sumOfWeights = 0;
                for (int z0 = 0; z0 < clusters.get(z).size(); z0++)
                {
                    /* Compute the sum of all coordinates. */
                    BLAS.axpy(WP.get(clusters.get(z).get(z0)), P.get(clusters.get(z).get(z0)), c);
                    /* Compute the sum of weights. */
                    sumOfWeights += WP.get(clusters.get(z).get(z0));
                }

                /* Compute the final average. */
                BLAS.scal((1.0/sumOfWeights),c);
                C.set(z, c);
            }

            /* Recompute currentDistances. */
            for (int z = 0; z < P.size(); z++)
            {
                double minDistance = Math.sqrt(Vectors.sqdist(C.get(0), P.get(z)));
                int minIndex = 0;
                for (int z0 = 1; z0 < k; z0++)
                {
                    double distance = Math.sqrt(Vectors.sqdist(C.get(z0), P.get(z)));
                    if (distance < minDistance)
                    {
                        minDistance = distance;
                        minIndex = z0;
                    }
                }
                currentDistances.get(z).distance = minDistance;
                currentDistances.get(z).index = minIndex;
            }
        }

        return C;
    }
}
