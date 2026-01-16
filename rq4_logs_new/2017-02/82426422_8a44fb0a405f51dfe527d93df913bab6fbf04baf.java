package KNN;

/**
 *
 * Created by Lucas Miguel S Ponce.
 * Student of master degree in Computer's Science
 * UFMG - 1/2017
 * Algorithm:  k-NN (K Nearst Neighborhood)
 *
 */

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class Knn {


    public static ArrayList<Sample> readFile(String file)  {

        long startTime = System.nanoTime();
        Sample sample;
        ArrayList<Sample> samples = new ArrayList<Sample>();
        int j =0;

        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = null; //reader.readLine(); // ignore header
            while((line = reader.readLine()) != null) {
                String[] tokens = line.split(",");
                if (tokens.length>1) {
                    if(j% 10000 == 0)
                        System.out.println("Line:"+j);
                    j++;
                    sample = new Sample();
                    sample.setLabel((int) Float.parseFloat(tokens[0]));
                    double[] col = new double[tokens.length - 1];
                    for (int i = 1; i < tokens.length; i++) {
                        col[i - 1] = Double.parseDouble(tokens[i]);
                    }
                    sample.setCols(col);
                    samples.add(sample);
                }
            }
            reader.close();
        } catch (FileNotFoundException e) {
            System.out.println("\n[ERROR] - readFile");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("\n[ERROR] - readFile");
            e.printStackTrace();
        }

        long estimatedTime0 = System.nanoTime() - startTime;
        double seconds0 = (double) estimatedTime0 / 1000000000.0;
        System.out.printf("\n[INFO] - readFile -> Time elapsed: %.2f seconds\n",seconds0);

        return samples;
    }

    public static int distance(double[] a, double[] b) {
        int sum = 0;
        for(int i = 0; i < a.length; i++) {
            sum += (a[i] - b[i]) * (a[i] - b[i]);
        }
        return (int)Math.sqrt(sum); // euclidian distance would be sqrt(sum)...
    }

    public static double getPopularElement(double[] a){
        int count = 1, tempCount;
        double popular = a[0];
        double temp;
        for (int i = 0; i < (a.length - 2); i++){
            temp = a[i];
            tempCount = 0;
            for (int j = 1; j < (a.length -1); j++){
                if (temp == a[j])
                    tempCount++;
            }
            if (tempCount > count){
                popular = temp;
                count = tempCount;
            }
        }
        return popular;
    }


    public static void classifyFrag(ArrayList<Sample> validationSet, ArrayList<Sample> trainingSet, int[] partial, int K){
        long startTime = System.nanoTime();

        int i=0;
        for(Sample sampleTest:validationSet) {

            if ( (i % 1000) == 0)
                System.out.println("Classifying: " + i);

            double [][] bestPoints = new double[2][K+1];
            for (int d=0; d<K;d++){
                bestPoints[0][d] = -1.0;
                bestPoints[1][d] = Double.MAX_VALUE;
            }

            double tmp_dist;
            double tmp_label;
            for (Sample sampleTrain : trainingSet) {
                    bestPoints[1][K - 1] = distance(sampleTest.getCols(), sampleTrain.getCols());
                    bestPoints[0][K - 1] = sampleTrain.getLabel();

                    for (int j = K; j > 0; j--) { //Insert Sort
                        if (bestPoints[1][j] < bestPoints[1][j - 1]) {
                            tmp_label = bestPoints[0][j];
                            bestPoints[0][j] = bestPoints[0][j - 1];
                            bestPoints[0][j - 1] = tmp_label;

                            tmp_dist = bestPoints[1][j];
                            bestPoints[1][j] = bestPoints[1][j - 1];
                            bestPoints[1][j - 1] = tmp_dist;
                        }
                    }

            }
            partial[i] = (int) getPopularElement(bestPoints[0]);
            i++;
        }

        long estimatedTime0 = System.nanoTime() - startTime;
        double seconds0 = (double) estimatedTime0 / 1000000000.0;
        System.out.printf("[INFO] - classifyFrag -> Time elapsed: %.2f seconds\n\n",seconds0);

    }

    public static void evaluateFrag( ArrayList<Sample>part, int[] partial, int[] correct){
        long startTime = System.nanoTime();

        for (int i = 0; i<part.size();i++) {
            if (partial[i] == part.get(i).getLabel())
                correct[0]++;
        }

        long estimatedTime = System.nanoTime() - startTime;
        double seconds = (double) estimatedTime / 1000000000.0;
        System.out.printf("[INFO] - evaluateFrag -> Time elapsed: %.2f seconds\n\n",seconds);

    }

    public static void accumulate(int[] numcorrect, int[] numcorrect2){

        for(int f=0; f<numcorrect2.length;f++)
            numcorrect[f] += numcorrect2[f];

    }

    public static void main(String[] args) throws IOException {

        int frag = 2;
        int K = 1;
        String trainingName = "/media/lucasmsp/Dados/TEMP/Dataset/higgs-train-0.1m.csv";
        String testName     = "/media/lucasmsp/Dados/TEMP/Dataset/higgs-test-0.1m.csv";


        // Get and parse arguments
        int argIndex = 0;
        while (argIndex < args.length) {
            String arg = args[argIndex++];
            if (arg.equals("-K")) {
                K = Integer.parseInt(args[argIndex++]);
            } else if (arg.equals("-f")) {
                frag = Integer.parseInt(args[argIndex++]);
            }
        }

        System.out.println("Running with the following parameters:");
        System.out.println("- Clusters: " + K);
        System.out.println("- Nodes: " + frag);
        System.out.println("- Training set: " + trainingName);
        System.out.println("- Test set: " + testName);


        ArrayList<Sample> trainingSet   = readFile(trainingName); //Only in Master
        ArrayList<Sample> TestSet       = readFile(testName); //Only in Master


        int TestSize = TestSet.size();
        int start = 0;
        int end = TestSize/frag;

        int[][] result = new int[frag][TestSize/frag + 1];
        int[][] numcorrect = new int[frag][1];

        int i =0;
        //Split the TestSet and classifying it
        for (int f = 0 ; f<frag;f++) {
            ArrayList<Sample> part = new ArrayList<Sample>(TestSet.subList(start, end));
            start = start + TestSet.size()/frag;
            end = end + TestSize/frag;
            classifyFrag(part,trainingSet,result[i],K);
            evaluateFrag(part,result[i],numcorrect[i]);
            i++;
        }

        //Accumulate Evaluation
        int size = frag;
        i = 0;
        int gap = 1;
        while (size > 1) {
            accumulate(numcorrect[i], numcorrect[i + gap]);
            size--;
            i = i + 2 * gap;
            if (i == frag) {
                gap *= 2;
                i = 0;
            }
        }

        System.out.println("Accuracy: " + (double) numcorrect[0][0] / TestSize * 100 + "%");

    }
}