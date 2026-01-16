package Q1;

import java.util.*;
import java.lang.Math;

//-----------------------------------------
// NAME: Future Hu
// STUDENT NUMBER: 7859844
// COURSE: COMP3190, SECTION: A01
// INSTRUCTOR: Dylan Fries
// ASSIGNMENT: 1, QUESTION: 1
//-----------------------------------------
public class A1Q1 {
    public static Stack<State> dfsStack;
    public static Queue<State> bfsQueue;

    public static void main(String[] args)
    {
        dfsStack = new Stack<>();
        bfsQueue = new LinkedList<>();
        Scanner keyboard = new Scanner(System.in);

        String[] puzzle;
        int[] board;
        State initial;

        puzzle = keyboard.nextLine().split(",");
        board = new int[puzzle.length];

        for (int i = 0; i < puzzle.length; i++)
            board[i] = Integer.parseInt(puzzle[i]);

        initial = new State(board, 0);

        // Solve puzzle
        System.out.println("Initial puzzle:\n" + initial.toString());
        System.out.println("# of states searched:" + depthFirstSearch(initial) + "\n");
        System.out.println("# of states searched:" + breadthFirstSearch(initial) + "\n");
        System.out.println("# of states searched:" + iterativeDeepening(initial) + "\n");
        System.out.println("# of states searched:" + informedSearch(initial) + "\n");

        System.out.println("Done");
    }

    /**
     * Solve puzzle using informed search
     *
     * @param initial Ths initial state (which is puzzle itself)
     * @return # of states searched during the process
     */
    public static long informedSearch(State initial) {
        long startTime, endTime;
        long stateChecked = 0;
        State solution = null;

        startTime = System.currentTimeMillis();

        // Initialize search stack
        dfsStack.clear();
        dfsStack.push(initial);

        // Evaluate each state and their sub-states
        while (!dfsStack.empty() && solution == null) {
            State now = dfsStack.pop();
            stateChecked++;

            // Get sub-states only if the current state is valid
            if (now.checkCurrent()) {
                if (now.checkSolution())
                    solution = now;

                for (State next : now.getNextSmarter())
                    dfsStack.push(next);
            }
        }

        endTime = System.currentTimeMillis();

        // Show the result
        if (solution != null) {
            System.out.println("Informed search done:");
            System.out.println(solution.toString());
            System.out.println("It takes " + (endTime - startTime) + " milli seconds.");
        } else {
            System.out.println("Informed search failed.");
        }

        dfsStack.clear();
        return stateChecked;
    }

    /**
     * Solve puzzle using depth-first search
     * It has almost the same structure as informed search
     *
     * @param initial Ths initial state (which is puzzle itself)
     * @return # of states searched during the process
     */
    public static long depthFirstSearch(State initial) {
        long startTime, endTime;
        long stateChecked = 0;
        State solution = null;

        startTime = System.currentTimeMillis();

        dfsStack.clear();
        dfsStack.push(initial);

        while (!dfsStack.empty() && solution == null) {
            State now = dfsStack.pop();
            stateChecked++;

            if (now.checkCurrent()) {
                if (now.checkSolution())
                    solution = now;

                for (State next : now.getNext())
                    dfsStack.push(next);
            }
        }

        endTime = System.currentTimeMillis();

        if (solution != null) {
            System.out.println("Depth first search done:");
            System.out.println(solution.toString());
            System.out.println("It takes " + (endTime - startTime) + " milli seconds.");
        } else {
            System.out.println("Depth first search failed.");
        }

        dfsStack.clear();
        return stateChecked;
    }

    /**
     * Solve puzzle using breadth-first search
     * It has almost the same structure as informed search
     *
     * @param initial Ths initial state (which is puzzle itself)
     * @return # of states searched during the process
     */
    public static long breadthFirstSearch(State initial) {
        long startTime, endTime;
        long stateChecked = 0;
        State solution = null;

        startTime = System.currentTimeMillis();

        bfsQueue.clear();
        bfsQueue.offer(initial);

        while (!bfsQueue.isEmpty() && solution == null) {
            State now = bfsQueue.poll();
            stateChecked++;

            if (now.checkCurrent()) {
                if (now.checkSolution())
                    solution = now;

                for (State next : now.getNext())
                    bfsQueue.offer(next);
            }
        }

        endTime = System.currentTimeMillis();

        if (solution != null) {
            System.out.println("Breadth first search done:");
            System.out.println(solution.toString());
            System.out.println("It takes " + (endTime - startTime) + " milli seconds.");
        } else {
            System.out.println("Breadth first search failed.");
        }

        bfsQueue.clear();
        return stateChecked;
    }

    /**
     * Solve puzzle using iterative deepening search
     * It has almost the same structure as informed search
     *
     * @param initial Ths initial state (which is puzzle itself)
     * @return # of states searched during the process
     */
    public static long iterativeDeepening(State initial) {
        long startTime, endTime;
        long stateChecked = 0;
        int maxDepth;
        State solution = null;

        startTime = System.currentTimeMillis();

        for (maxDepth = 1; maxDepth <= initial.maxDepth() && solution == null; maxDepth++) {
            dfsStack.clear();
            dfsStack.push(initial);

            while (!dfsStack.empty() && solution == null) {
                State now = dfsStack.pop();
                stateChecked++;


                if (now.checkCurrent()) {
                    if (now.checkSolution())
                        solution = now;

                    // Do not exceed the max depth
                    if (now.getDepth() < maxDepth)
                        for (State next : now.getNext())
                            dfsStack.push(next);
                }
            }
        }

        endTime = System.currentTimeMillis();

        if (solution != null) {
            System.out.println("Iterative deepening search done:");
            System.out.println(solution.toString());
            System.out.println("It takes " + (endTime - startTime) + " milli seconds.");
            System.out.println("The solution is found at depth " + maxDepth);
        } else {
            System.out.println("Iterative deepening search failed.");
        }

        dfsStack.clear();
        return stateChecked;
    }
}

/**
 * This is state,
 * It represents the whole board (puzzle)
 * at a specific time in the searching process
 */
class State {
    private int[] board;    // Numbers in the board
    private int[][] triangularForm; // A triangular-shaped version of the previous variable
    private int depth;  // Depth of the current state (at which 0 are we dealing with?)
    private int size;   // How many numbers are in this triangle?
    private int baseLength; // Base length of the triangle (should be same as height)

    /**
     * Accessor
     */
    public int getDepth() { return depth; }

    /**
     * The maximum depth we can go,
     * in other words, the # of 0 in initial puzzle
     * @return max depth
     */
    public int maxDepth() {
        int maxDepth = 0;
        for (int i = 0; i < size; i++)
            maxDepth += (board[i] == 0) ? 1 : 0;

        return maxDepth;
    }

    /**
     * Constructor
     * @param board The board at this specific time (after solving a few 0's)
     * @param depth The depth of this state
     */
    public State(int[] board, int depth){
        this.board = board;
        this.depth = depth;

        size = board.length;
        baseLength = ((int) Math.sqrt(size * 8 + 1) - 1) / 2;   // This equation calculates the base length

        // Create a triangular form
        triangularForm = new int[baseLength][baseLength];
        for (int row = 0, i = 0; row < baseLength; row++) {
            for (int col = 0; col <= row; col++) {
                triangularForm[row][col] = board[i++];
            }
        }
    }

    /**
     * Get the next possible states.
     * @return A list of next possible states
     */
    public ArrayList<State> getNext() {
        ArrayList<State> next = new ArrayList<>();
        int firstZero;  // Index of first zero

        // Locate the first zero
        for (firstZero = 0; firstZero < size && board[firstZero] != 0; firstZero++) {}

        // There is a 0 in the board and we found it
        if (firstZero < size && board[firstZero] == 0) {

            // Generate next states by changing 0 to 1-9
            for (int i = 1; i <= 9; i++) {
                int[] newBoard = board.clone();
                newBoard[firstZero] = i;
                State newState= new State(newBoard, depth + 1);
                next.add(newState);
            }
        }

        return next;
    }

    /**
     * A smarter version of previous method.
     * Informed search exclusive.
     * @return A list of next possible states
     */
    public ArrayList<State> getNextSmarter() {
        ArrayList<State> next = new ArrayList<>();
        int firstZero = 0;  // Index of first zero
        int firstZeroRow = 0, firstZeroCol = 0; // Index of first zero in triangular form
        int leftNeighbour, upNeighbour;
        boolean firstZeroFound = false;

        // Find index of the first zero number in triangular form
        for (int r = 0, i = 0; r < baseLength && !firstZeroFound; r++) {
            for (int c = 0; c <= r && !firstZeroFound; c++, i++) {
                if (triangularForm[r][c] == 0) {
                    firstZeroFound = true;
                    firstZeroRow = r;
                    firstZeroCol = c;
                    firstZero = i;
                }
            }
        }

        if (firstZeroFound)  {
            // The potential candidates of next states
            // Set doesn't allow duplicates, which makes it handy
            Set<Integer> candidate = new HashSet<>();

            // When the first zero is not at the left edge of triangle,
            // find its potential sub-states by looking at its left and up neighbour
            if (firstZeroCol != 0) {
                leftNeighbour = triangularForm[firstZeroRow][firstZeroCol - 1];
                upNeighbour = triangularForm[firstZeroRow - 1][firstZeroCol - 1];

                if (leftNeighbour + upNeighbour <= 9)
                    candidate.add(leftNeighbour + upNeighbour);

                if (Math.abs(leftNeighbour - upNeighbour) <= 9)
                    candidate.add(Math.abs(leftNeighbour - upNeighbour));

                if (leftNeighbour * upNeighbour <= 9)
                    candidate.add(leftNeighbour * upNeighbour);

                if ((leftNeighbour / (float)upNeighbour) % 1 == 0)
                    candidate.add(leftNeighbour / upNeighbour);

                if ((upNeighbour / (float)leftNeighbour) % 1 == 0)
                    candidate.add(upNeighbour / leftNeighbour);

                candidate.remove(0);

            }
            else {   // If it is at the left edge, do the regular getNext()
                for (int i = 1; i <= 9; i++)
                    candidate.add(i);
            }

            // Generate next states
            for (int item: candidate) {
                int[] newBoard = board.clone();
                newBoard[firstZero] = item;
                State newState= new State(newBoard, depth + 1);
                next.add(newState);
            }

        }

        return next;
    }

    /**
     * Check the current state is valid or not
     * "Valid" means, there is no violation of game rules YET
     * @return Yes or No
     */
    public boolean checkCurrent() {
        boolean result = true;
        boolean zeroFound = false;

        // For all numbers before the first 0
        // Check that they don't violate any operation of game
        for (int row = 0; row < baseLength - 1 && result && !zeroFound; row++) {
            for (int col = 0; col <= row && result && !zeroFound; col++) {
                int thisNum = triangularForm[row][col];
                int nextNum1 = triangularForm[row + 1][col];
                int nextNum2 = triangularForm[row + 1][col + 1];

                if (thisNum != 0 && nextNum1 != 0 && nextNum2 != 0) {
                    result = (thisNum == nextNum1 + nextNum2 ||
                            thisNum == Math.abs(nextNum1 - nextNum2) ||
                            thisNum == nextNum1 * nextNum2 ||
                            thisNum == nextNum1 / (float) nextNum2 ||
                            thisNum == nextNum2 / (float) nextNum1);
                } else
                    zeroFound = true;
            }
        }

        // Check that each layer of the triangle doesn't have duplicated numbers
        // 0's are ignored
        for (int row = 1; row < baseLength && result; row++) {
            ArrayList<Integer> check = new ArrayList<>();
            int[] line = triangularForm[row].clone();

            for (int col = 0; col <= row && result; col++) {
                if (!check.contains(line[col]) || line[col] == 0)
                    check.add(line[col]);
                else
                    result = false;
            }
        }

        return result;
    }

    // Since checkCurrent() guarantees all non-zero numbers are good
    // Then no 0's means we have a solution
    public boolean checkSolution() {
        boolean result = true;

        for (int i = 0; i < board.length && result; i++) {
            if (board[i] == 0)
                result = false;
        }

        return result;
    }

    /**
     * toString: Show the current state
     */
    public String toString() {
        String result = "";

        for (int row = 0; row < baseLength; row++) {
            for (int col = 0; col <= row; col++) {
                result += triangularForm[row][col] + " ";
            }
            result += "\n";
        }

        return result;
    }
}