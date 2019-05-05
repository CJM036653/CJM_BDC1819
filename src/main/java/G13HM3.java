import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Random;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.BLAS;

public class G13HM3
{
    /******************** MAIN ********************/
    public static void main(String[] args)
    {
        ArrayList<Vector> P = new ArrayList<>();
        int k = 0;
        int iter = 0;

        if (args.length != 3)
        {
            System.out.println("Usage: <filename> <k> <iter>");
            System.exit(1);
        }
        try
        {
            P = readVectorsSeq(args[0]);
            k = Integer.parseInt(args[1]);
            iter = Integer.parseInt(args[2]);
        }
        catch (IOException e)
        {
            System.out.println("IO Error");
            System.exit(1);
        }
        catch (NumberFormatException e)
        {
            System.out.println("Usage: <filename> <k> <iter>");
            System.exit(1);
        }

        /* Init of the weights to 1. */
        ArrayList<Long> WP = new ArrayList<>(P.size());
        for (int i = 0; i < P.size(); i++)
        {
            WP.add(1L);
        }

        /* Compute the centres. */
        ArrayList<Vector> C = kmeansPP(P,WP,k,iter);

        /* Print the average distance of points from P to C. */
        System.out.println("The average distance between points in P and their centres is: " + kmeansObj(P,C));
    }


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
        /* Set of weights corresponding to the centres. */
        ArrayList<Long> C_WP = new ArrayList<>(k);

        /******************** KMEANS++ **********************/
        /* Add first random element with uniform distribution and remove it from P. */
        Random randomGenerator = new Random();
        int randomInt = randomGenerator.nextInt(P.size());
        Vector temp = P.remove(randomInt);
        long temp_wp = WP.remove(randomInt);
        C.add(temp);
        C_WP.add(temp_wp);

        /* Store the minimum distance of all points in P from C. */
        ArrayList<Pair> currentDistances = new ArrayList<>(P.size());
        /* Compute the minimum distance of all points in P from C. */
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

            boolean done = false;
            while (!done)
            {
                double newProbabilitySum = probabilitySum + (WP.get(j) * currentDistances.get(j).distance)/sumOfDistances;
                if (newProbabilitySum >= chosenValue)
                {
                    Vector chosenPoint = P.remove(j);
                    currentDistances.remove(j);
                    long chosenPoint_WP = WP.remove(j);
                    C.add(chosenPoint);
                    C_WP.add(chosenPoint_WP);
                    done = true;
                }
                else
                {
                    probabilitySum = newProbabilitySum;
                    j++;
                }
            }

            /* Recompute the minimum distance of points in P from C. */
            for (j = 0; j < P.size(); j++)
            {
                double distance = Math.sqrt(Vectors.sqdist(P.get(j), C.get(i)));
                if (distance < currentDistances.get(j).distance) currentDistances.set(j, new Pair(distance, i));
            }
        }

        /*********************** LLOYD **************************/
        /* Restore P, WP and currentDistances with the centres selected by Kmeans++ */
        P.addAll(C);
        WP.addAll(C_WP);
        for (int z = 0; z < k; z++)
        {
            currentDistances.add(new Pair(0,z));
        }
        /* Current value of the objective function. (The coefficient 1/|P| is not relevant.) */
        double phi = kmeansObj(P,C);

        for (int i = 0; i < iter; i++)
        {
            /* Dataset clusters. */
            ArrayList<ArrayList<Vector>> clusters = new ArrayList<>(k);

            /* Partition(P,C) */
            /* Create clusters which contain their centres. */
            for (int z = 0; z < k; z++)
            {
                clusters.add(new ArrayList<>());
                clusters.get(z).add(C.get(z));
            }
            /* Add each point to its cluster. */
            for (int z = 0; z < P.size(); z++)
            {
                clusters.get(currentDistances.get(z).index).add(P.get(z));
            }

            /* Compute new centroids. */
            ArrayList<Vector> newC = new ArrayList<>(k);
            for (int z = 0; z < k; z++)
            {
                Vector c = Vectors.zeros(P.get(0).size());
                for (int z0 = 0; z0 < clusters.get(z).size(); z0++)
                {
                    BLAS.axpy(1,clusters.get(z).get(z0), c);
                }
                BLAS.scal((1/clusters.get(z).size()),c);
                newC.add(c);
            }

            double newPhi = kmeansObj(P,newC);
            if (newPhi < phi)
            {
                phi = newPhi;
                C = newC;
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
        }

        return C;
    }


    public static double kmeansObj(ArrayList<Vector> P, ArrayList<Vector> C)
    {
        double sumOfDistances = 0;

        for (int i = 0; i < P.size(); i++)
        {
            double minDistance = Math.sqrt(Vectors.sqdist(C.get(0), P.get(i)));
            for (int j = 1; j < C.size(); j++)
            {
                double distance = Math.sqrt(Vectors.sqdist(C.get(j), P.get(i)));
                if (distance < minDistance)
                {
                    minDistance = distance;
                }
            }
            sumOfDistances += minDistance;
        }

        return sumOfDistances/P.size();
    }

    /* Input functions. */
    public static Vector strToVector(String str)
    {
        String[] tokens = str.split(" ");
        double[] data = new double[tokens.length];
        for (int i=0; i<tokens.length; i++)
        {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return Vectors.dense(data);
    }

    public static ArrayList<Vector> readVectorsSeq(String filename) throws IOException
    {
        if (Files.isDirectory(Paths.get(filename)))
        {
            throw new IllegalArgumentException("readVectorsSeq is meant to read a single file.");
        }
        ArrayList<Vector> result = new ArrayList<>();
        Files.lines(Paths.get(filename))
                .map(str -> strToVector(str))
                .forEach(e -> result.add(e));
        return result;
    }
}
