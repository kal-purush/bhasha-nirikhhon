/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package model;

import gui_timetabling.Line;
import gui_timetabling.MultipleLinesChart;
import java.util.ArrayList;
import util.DataIO;
import static model.ExamineTimetablingProblem.*;
import util.UtilManager;

/**
 *
 * @author quancq
 */
public class Statistics {

    public static ExamineTimetablingProblem Timetabling;

    public static void main(String[] args) {
        Timetabling = new ExamineTimetablingProblem();
        System.out.println(Timetabling.manager.toString());

        // Set up problem's model
        Timetabling.stateModel();
//        
//        Timetabling.testBatchAnnealing(1);
        tabuStatistic();

    }

    public static void separateAlgorithmStatistic() {
    }

    public static void tabuStatistic() {

        //
        int[] tabuLenArr = new int[]{20, 40};
        int[] maxTimeArr = new int[]{15, 20};
        int[] maxIterArr = new int[]{30000, 50000};
        int[] maxStableArr = new int[]{200, 300};
        int numParameters = tabuLenArr.length;
        int numTest = 2;

        // statistic
        String dirSaveFile = "src/statistics/" + System.currentTimeMillis();
        DataIO.makeDir(dirSaveFile);

        ArrayList<ArrayList<String>> outputList = new ArrayList<>();
        ArrayList<String> colName = new ArrayList<>();
        colName.add("ParameterID");
        colName.add("TabuLen");
        colName.add("maxTime");
        colName.add("maxIter");
        colName.add("maxStable");
        colName.add("MinFitness");
        colName.add("AverageFitness");
        colName.add("MaxFitness");
        colName.add("VarianceFitness");
        colName.add("MinTime");
        colName.add("AverageTime");
        colName.add("MaxTime");
        colName.add("VarianceTime");

        outputList.add(colName);

        for (int indexPara = 0; indexPara < numParameters; ++indexPara) {
            String paraID = "Parameter" + (indexPara + 1);

            // set parameter
            tabulen = tabuLenArr[indexPara];
            maxTime = maxTimeArr[indexPara];
            maxIter = maxIterArr[indexPara];
            maxStable = maxStableArr[indexPara];

            Timetabling.testBatchTabu(numTest);

            ArrayList<String> row1 = new ArrayList<>();

//                row1.add("Parameter1");
            row1.add(paraID);
            row1.add(String.valueOf(tabulen));
            row1.add(String.valueOf(maxTime));
            row1.add(String.valueOf(maxIter));
            row1.add(String.valueOf(maxStable));

            double minFitness = UtilManager.getMin(bestFitness);
            double avgFitness = UtilManager.getAverage(bestFitness);
            double maxFitness = UtilManager.getMax(bestFitness);
            double varFitness = UtilManager.getVariance(bestFitness);

            double minTime = UtilManager.getMin(timeRun);
            double avgTime = UtilManager.getAverage(timeRun);
            double maxTime = UtilManager.getMax(timeRun);
            double varTime = UtilManager.getVariance(timeRun);

            row1.add(String.valueOf(minFitness));
            row1.add(String.valueOf(avgFitness));
            row1.add(String.valueOf(maxFitness));
            row1.add(String.valueOf(varFitness));

            System.out.println("\n==>" + timeRun);
            row1.add(String.valueOf(minTime));
            row1.add(String.valueOf(avgTime));
            row1.add(String.valueOf(maxTime));
            row1.add(String.valueOf(varTime));
            System.out.println("\nRow1 = " + row1);

            outputList.add(row1);

            boolean isShowGui = true;
            if (isShowGui) {
                // show violation_loop
                MultipleLinesChart mlc = new MultipleLinesChart("Demo");
                mlc.setxAxisLabel("Loop");
                mlc.setyAxisLabel("Violation");
                mlc.setChartTitle("Tabu algorithm");

                ArrayList<Line> lineList = new ArrayList<>();
                ArrayList<Double> xList = new ArrayList<>();
                ArrayList<Double> yList = new ArrayList<>();

                int index = 0;
                yList = violationList.get(index);

                for (int k = 0; k < yList.size(); ++k) {
                    xList.add(1.0 * (k + 1));
                }

                Line lineViolation = new Line("Violation", xList, yList);
                lineList.add(lineViolation);

                mlc.setLineList(lineList);

                String pathSaveImage = dirSaveFile + "/violation-loop_" + paraID + ".png";
                mlc.renderGraph(false, pathSaveImage);

                // show violation_loop
                MultipleLinesChart mlc2 = new MultipleLinesChart("Demo");
                mlc2.setxAxisLabel("Loop");
                mlc2.setyAxisLabel("Violation");
                mlc2.setChartTitle("Tabu algorithm");

                ArrayList<Line> lineListFitness = new ArrayList<>();
                ArrayList<Double> xList2 = new ArrayList<>();
                ArrayList<Double> yList2 = new ArrayList<>();

                int index2 = 0;
                yList2 = fitnessList.get(index2);

                for (int k = 0; k < yList2.size(); ++k) {
                    xList2.add(1.0 * (k + 1));
                }

                Line lineFitness = new Line("Fitness", xList2, yList2);
                lineListFitness.add(lineFitness);
                mlc2.setLineList(lineListFitness);

                String pathSaveImage2 = dirSaveFile + "/fitness-loop_" + paraID + ".png";
                mlc2.renderGraph(false, pathSaveImage2);
            }

        }
        // write excel file
        String csvPath = dirSaveFile + "/" + Timetabling.manager.getDatasetName() + "_NumTest-" + numTest + ".csv";
        DataIO.writeFileExcel(csvPath, outputList);

        // write infomation data file
        String infoDataPath = dirSaveFile + "/info_data.txt";
        DataIO.writeStringToFile(infoDataPath, Timetabling.manager.toString());

    }
}