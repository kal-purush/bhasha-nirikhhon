package com.makebono.algorithms.hungarianalgorithm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

import com.makebono.algorithms.hungarianalgorithm.tools.HungarianNode;
import com.makebono.algorithms.hungarianalgorithm.tools.HungarianTree;
import com.makebono.datastructures.matrix.matrix.BonoMatrix2D;
import com.makebono.datastructures.matrix.matrixinterface.Matrix;

/** 
 * @ClassName: HungarianWorkDistributor 
 * @Description: Assign work by Hungarian algorithm.
 * @author makebono
 * @date 2017年11月20日 下午4:30:15 
 *  
 */
public class HungarianWorkDistributor {
    private final Matrix table;
    private int workerNum;
    private int workNum;
    private final int workerCapacity;
    private final int workCapacity;
    private final String[] workerList;
    private final String[] workList;

    public HungarianWorkDistributor(final int workerCapacity, final int workCapacity) {
        this.table = new BonoMatrix2D(workerCapacity, workCapacity);
        this.workerCapacity = workerCapacity;
        this.workCapacity = workCapacity;
        this.workerList = new String[workerCapacity];
        this.workList = new String[workCapacity];
        this.workerNum = 0;
        this.workNum = 0;

        this.situationWizard();
    }

    public void solve() {
        final int[][] temp = this.getTable().cloneMatrix();

        for (int i = 0; i < temp.length; i++) {
            int tempMin = temp[i][0];
            for (final int buffer : temp[i]) {
                if (tempMin > buffer) {
                    tempMin = buffer;
                }
            }
            for (int o = 0; o < temp[i].length; o++) {
                temp[i][o] -= tempMin;
            }
        }

        for (int i = 0; i < temp[0].length; i++) {
            int tempMin = temp[0][i];
            for (int o = 0; o < temp[0].length; o++) {
                if (tempMin > temp[o][i]) {
                    tempMin = temp[o][i];
                }
            }
            for (int o = 0; o < temp[0].length; o++) {
                temp[o][i] -= tempMin;
            }
        }

        for (int i = 0; i < temp.length; i++) {
            for (final int cursor : temp[i]) {
                System.out.print(cursor + " ");
            }
            System.out.println();
        }

        boolean optimized = false;
        HungarianTree solution = new HungarianTree(temp.length);
        solution.buildTree(temp);
        ArrayList<HungarianNode> answer = solution.findSolution();
        optimized = answer.size() != 0;

        // System.out.println(answer.size());

        /*
        System.out.println("Tree size: " + solution.size());     
        System.out.println("Answer size: " + answer.size());
        System.out.println("Optimal solution found. Shows below:");
        
        for (final HungarianNode cursor : answer) {
            System.out.println("(" + cursor.getM() + ", " + cursor.getN() + ")");
        }
        */

        while (!optimized) {
            final int[][] draw = HungarianWorkDistributor.draw(temp);
            HungarianWorkDistributor.updateTempTable(temp, draw);

            for (int i = 0; i < temp.length; i++) {
                for (final int cursor : temp[i]) {
                    System.out.print(cursor + " ");
                }
                System.out.println();
            }

            solution = new HungarianTree(temp.length);
            solution.buildTree(temp);
            answer = solution.findSolution();

            optimized = answer.size() != 0;
        }

        this.printAnswer(answer);
    }

    // Needs to tick column for not only the first unassigned row, but also following rows ticked by columns.
    public static int[][] draw(final int[][] temp) {
        final int[][] draw = new int[temp.length][temp[0].length];
        boolean conflict = false;
        final int[] markedRow = new int[temp.length];
        final int[] markedCol = new int[temp[0].length];

        for (int i = 0; i < draw.length; i++) {
            Arrays.fill(draw[i], 0);
        }

        Arrays.fill(markedCol, 1);
        Arrays.fill(markedRow, 0);

        boolean newRowTicked = false;

        // 1. Scan through the matrix and check assignable work from top to bottom. Avoid conflict and mark
        // unassigned rows.
        for (int i = 0; i < temp.length; i++) {
            conflict = false;
            for (int o = 0; o < temp[0].length; o++) {

                if (temp[i][o] == 0) {
                    // Looking for conflict.
                    for (int p = 0; p < i; p++) {
                        if (temp[p][o] == 0 && draw[p][o] == 1) {
                            conflict = true;
                        }
                    }

                    if (!conflict) {
                        draw[i][o] = 1;
                        break;
                    }
                }
            }
            if (conflict) {
                markedRow[i] = 1;
                newRowTicked = true;
            }
        }

        // Modify here.
        while (newRowTicked) {
            /*
            for (int i = 0; i < markedRow.length; i++) {
                System.out.println(markedRow[i]);
            }
            
            for (int i = 0; i < draw.length; i++) {
                for (int o = 0; o < draw[0].length; o++) {
                    System.out.print(draw[i][o] + " ");
                }
                System.out.println();
            }
            */

            // 2. Scan through the unassigned rows and try to mark relative columns.
            for (int i = 0; i < markedRow.length; i++) {
                if (markedRow[i] == 1) {
                    for (int o = 0; o < markedCol.length; o++) {
                        if (temp[i][o] == 0) {
                            markedCol[o] = 0;
                        }
                    }
                }
            }

            /*
            for (int i = 0; i < markedCol.length; i++) {
                System.out.println(markedCol[i]);
            }
            */

            // 3. Mark rows having assignments in columns marked above.
            for (int o = 0; o < markedCol.length; o++) {
                if (markedCol[o] == 0) {
                    for (int i = 0; i < markedRow.length; i++) {
                        if (temp[i][o] == 0 && draw[i][o] == 1) {
                            markedRow[i] = 1;
                        }
                    }
                }
            }

            /*
            for (int i = 0; i < markedRow.length; i++) {
                System.out.println(markedRow[i]);
            }
            for (int i = 0; i < markedRow.length; i++) {
                System.out.println(markedCol[i]);
            }
            */
        }

        // These 3 steps finished the marking part. Below goes the drawing part.

        for (int i = 0; i < draw.length; i++) {
            Arrays.fill(draw[i], 0);
        }

        for (int i = 0; i < draw.length; i++) {
            if (markedRow[i] == 0) {
                Arrays.fill(draw[i], 1);
            }
        }

        for (int o = 0; o < draw[0].length; o++) {
            if (markedCol[o] == 0) {
                for (int i = 0; i < draw.length; i++) {
                    draw[i][o] += 1;
                }
            }
        }

        return draw;
    }

    public static int unselectedRegionMin(final int[][] table, final int[][] draw) {
        int min = Integer.MAX_VALUE;
        for (int i = 0; i < table.length; i++) {
            for (int o = 0; o < table[0].length; o++) {
                if (draw[i][o] == 0 && table[i][o] < min) {
                    min = table[i][o];
                }
            }
        }

        return min;
    }

    public static void updateTempTable(final int[][] table, final int[][] draw) {
        final int unselectedRegionMin = HungarianWorkDistributor.unselectedRegionMin(table, draw);

        for (int i = 0; i < table.length; i++) {
            for (int o = 0; o < table[0].length; o++) {
                if (draw[i][o] == 0) {
                    table[i][o] -= unselectedRegionMin;
                }

                if (draw[i][o] == 2) {
                    table[i][o] += unselectedRegionMin;
                }
            }
        }
    }

    public Matrix getTable() {
        return this.table;
    }

    public int getWorkerNum() {
        return this.workerNum;
    }

    public int getWorkNum() {
        return this.workNum;
    }

    public String[] getWorkerList() {
        return this.workerList;
    }

    public String[] getWorkList() {
        return this.workList;
    }

    public void addWorker(final String name) {
        if (this.workerNum < this.workerCapacity) {
            this.workerList[this.workerNum] = name;
            this.workerNum++;
        }
    }

    public void addWork(final String name) {
        if (this.workNum < this.workCapacity) {
            this.workList[this.workNum] = name;
            this.workNum++;
        }
    }

    private void situationWizard() {
        final Scanner sc = new Scanner(System.in);
        System.out.printf("Wizard for situation set-up.\nPlease enter your workers' names");

        while (this.workerNum != this.workerCapacity) {
            System.out.printf("\n    Input your next worker's name: ");
            this.addWorker(sc.next());
        }

        System.out.printf("\n\nPlease enter your works' names");
        while (this.workNum != this.workCapacity) {
            System.out.printf("\n    Input your next work's name: ");
            this.addWork(sc.next());
        }

        System.out.printf("\n\nPlease enter the costs of works for each worker");
        for (int i = 0; i < this.workerCapacity; i++) {
            for (int o = 0; o < this.workCapacity; o++) {
                System.out.printf("\n    Input the cost of " + this.getWorkerList()[i] + " doing "
                        + this.getWorkList()[o] + ": ");
                this.getTable().set(i, o, sc.nextInt());
            }
        }

        sc.close();
    }

    private void printAnswer(final ArrayList<HungarianNode> answer) {
        System.out.println("Optimal solution found. Shows below:");
        for (final HungarianNode cursor : answer) {
            System.out.println("(" + cursor.getM() + ", " + cursor.getN() + ")");
        }
    }

    // Align this later.
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Situation is like below: \n            ");
        for (int i = 0; i < this.workCapacity; i++) {
            sb.append(this.getWorkList()[i] + "    ");
        }
        for (int i = 0; i < this.workerCapacity; i++) {
            sb.append("\n    " + this.getWorkerList()[i] + "    ");
            for (int n = 0; n < this.workCapacity; n++) {
                sb.append(this.getTable().get(i, n).getValue() + "    ");
            }
        }

        return sb.toString();
    }
}