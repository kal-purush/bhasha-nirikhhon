/**
 * @Author Eligijus Skersonas
 * @Id 19335661
 *
 */
public class AnalysePascal {

    static int depth = 0;
    static int maxDepth = 0;
    static int overflows = 0;
    static int underflows = 0;
    static int windowsUsed = 2;
    static int numberOfWindows = 0;
    static int numberOfCalls = 0;
    static final int MIN_ACTIVE_WINDOWS = 2;

    public static void main(String[] args) {
        // Analyse Pascal with 6 overlapping register windows
        numberOfWindows = 6;
        analysePascal();

        // Analyse Pascal with 8 overlapping register windows
        numberOfWindows = 8;
        analysePascal();

        // Analyse Pascal with 16 overlapping register windows
        numberOfWindows = 16;
        analysePascal();

        // Time analysis on my computer
        // SYSTEM INFORMATION
        System.out.println("\nSYSTEM INFORMATION");
        System.out.println("Operating System: macOS Big Sur");
        System.out.println("Processor: 1.4 GHz Quad-Core Intel Core i5");
        System.out.println("Memory: 8 GB 2133 MHz LPDDR3");
        System.out.println("\nExecuting computePascal(30,20)...");
        long startTime = System.nanoTime();
        computePascal(30,20);
        long endTime = System.nanoTime();
        System.out.println("Execution time: " + (endTime-startTime)/1000000 + "ms");

    }

    public static int riscComputePascal(int row, int position)
    {
        onEntry();

        if(position == 1 || position == row) {
            onExit();
            return 1;
        }
        else {
            int left = riscComputePascal(row - 1, position);
            int right = riscComputePascal(row - 1, position - 1);
            onExit();
            return left+right;
        }
    }

    public static int computePascal(int row, int position)
    {
        if(position == 1 || position == row)
            return 1;
        else {
            int left = computePascal(row - 1, position);
            int right = computePascal(row - 1, position - 1);
            return left+right;
        }
    }

    private static void onEntry(){
        depth++;
        numberOfCalls++;
        if(depth > maxDepth) maxDepth = depth;
        if(windowsUsed == numberOfWindows) overflows++;
        else windowsUsed++;
    }

    private static void onExit(){
        depth--;
        if(windowsUsed == MIN_ACTIVE_WINDOWS) underflows++;
        else windowsUsed--;
    }

    private static void analysePascal(){
        System.out.println("\nOVERFLOW WHEN ALL REGISTERS UTILISED");
        System.out.println("Executing computePascal(30,20)...");
        System.out.println("Result: " + riscComputePascal(30,20));
        System.out.println("Number of Overlapping Windows: " + numberOfWindows);
        System.out.println("Number of procedure calls: " + numberOfCalls);
        System.out.println("Maximum depth: " + maxDepth);
        System.out.println("Number of Overflows: " + overflows);
        System.out.println("Number of Underflows: " + underflows);
        resetVariables();

        System.out.println("\nOVERFLOW WHEN ONE REGISTER EMPTY");
        System.out.println("Executing computePascal(30,20)...");
        numberOfWindows--; // this is the same as overflowing when 1 register is empty
        System.out.println("Result: " + riscComputePascal(30,20));
        numberOfWindows++; // restore original value
        System.out.println("Number of Overlapping Windows: " + numberOfWindows);
        System.out.println("Number of procedure calls: " + numberOfCalls);
        System.out.println("Maximum depth: " + maxDepth);
        System.out.println("Number of Overflows: " + overflows);
        System.out.println("Number of Underflows: " + underflows);
        resetVariables();
    }

    private static void resetVariables(){
        maxDepth = 0;
        overflows = 0;
        underflows = 0;
        numberOfCalls = 0;
    }
}