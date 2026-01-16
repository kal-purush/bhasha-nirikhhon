import java.io.File;

/**
 * 
 */

/**
 * EN605.202 Lab 4 Program Entry Point
 * Program runs solely at the command-line.  See readme.md for more information.
 * @author					Tom Atwood
 * @version					1.0.0.0
 * @since					1.0.0.0
 */
public class EN605202Lab4 {

	/**
	 * Program entry point.  Please see readme.md for parameter descriptions.
	 * @author				Tom Atwood
	 * @version				1.0.0.0
	 * @since				1.0.0.0
	 * @param args			Command-line arguments provided by user.
	 */
	public static void main(String[] args) {
		// Initialize method variables
		// These are the variables we're looking to populate and verify the values
		String sortType = "";
		String inputFile = "";
		String outputFile = "";
		
		// If no command-line parameters provided, provide quick guidance to help screen. 
		if (args.length==0) {
			validationFailureScreen("EN605202Lab4.java requires command-line parameters.  Please type 'EN605202Lab4 /?' for more help.");
			return;
		}
		
		// If help options provided in first parameter, show help screen.
		if ((args[0]=="help") ||
				(args[0]=="/?") ||
				(args[0]=="-help") ||
				(args[0]=="--help")) {
			helpScreen();
			return;
		}

		// Validate that a proper sort option is provided in the first parameter.
		if((args[0]=="HeapSort") ||
				(args[0]=="QuickSort") ||
				(args[0]=="QuickSortInsertionSortHybrid50") ||
				(args[0]=="QuickSortInsertionSortHybrid100") ||
				(args[0]=="QuickSortMedianOfThree")) {
			sortType = args[0];
		}
		else
		{
			validationFailureScreen("Invalid sort type provided in the first parameter.  Please type 'EN605202Lab4 /?' for more help.");
		}
		
		// Validate an input file is provided in the second parameter and that it is valid.
		if(args[1]==null) {
			validationFailureScreen("Input file path is required in parameter 1.");
			return;
		}

		if(!fileExists(inputFile)) {
			validationFailureScreen("Input file filepath is invalid.");
		}

		// Validate an output file is provided in the third parameter and that it is valid.
		if(args[2]==null) {
			validationFailureScreen("Output file path is required in parameter 2.");
			return;
		}
		
		if(!fileExists(outputFile)) {
			validationFailureScreen("Output file filepath is invalid.");					
		}
		
		// OK, now it's time to work!
		// Get input file contents.
		ArrayList<Integer> inputs = FileManager.GetFileLines(inputFile);
		int[] intArray = toPrimitive(inputs.toArray());

		// Run sort and output to file!
		switch(sortType.toLowerCase()) {
			case "heapsort": {
				HeapSort heapSort = new HeapSort();
				heapSort.iterativeHeapSort(intArray, true);
				heapSort.printArrayToFile(outputFile, intArray);
			}
			case "quicksort": {
				QuickSort quickSort = new QuickSort();
				quickSort.iterativeQuickSort(intArray, 2);
				quickSort.printArrayToFile(outputFile, intArray);
			}
			case "quicksortinsertionsorthybrid50": {
				QuickSortInsertionSortHybrid quickSortInsertionSortHybrid = new QuickSortInsertionSortHybrid();
				quickSortInsertionSortHybrid.iterativeQuickSort(intArray, 50);
				quickSortInsertionSortHybrid.printArrayToFile(outputFile, intArray);
			}
			case "quicksortinsertionsorthybrid100": {
				QuickSortInsertionSortHybrid quickSortInsertionSortHybrid = new QuickSortInsertionSortHybrid();
				quickSortInsertionSortHybrid.iterativeQuickSort(intArray, 100);
				quickSortInsertionSortHybrid.printArrayToFile(outputFile, intArray);
			}
			case "quicksortmedianofthree": {
				QuickSortMedianOfThree quickSortMedianOfThree = new QuickSortMedianOfThree();
				quickSortMedianOfThree.iterativeQuickSort(intArray, 2);
				quickSortMedianOfThree.printArrayToFile(outputFile, intArray);
			}
			default: {}
		}
	}	
	
	/**
	 * Clears the screen.
	 * @author				Tom Atwood
	 * @version				1.0.0.0
	 * @since				1.0.0.0
	 */
	private static void clearScreen() {
		System.out.print('\u000C');
	}

	/**
	 * Determines whether a file exists at the given filepath. 
	 * @author				Tom Atwood
	 * @version				1.0.0.0
	 * @since				1.0.0.0
	 * @param filepath		File path of file being sought.
	 * @return				Returns @true if file exists, else @false.
	 */
	private static boolean fileExists(String filepath) {
		File f = new File(filepath);
		if(f.exists() && !f.isDirectory()) { 
		    return true;
		}
		else { 
		    return false;
		}
	}
	
	/**
	 * Displays a help screen with information regarding options for command-line parameters.
	 * @author				Tom Atwood
	 * @version				1.0.0.0
	 * @since				1.0.0.0
	 */
	private static void helpScreen() {
		clearScreen();
		writeLine("EN605.202.81 Data Structures, Spring 2017, Lab #4 - Help Screen");
		writeLine("");
		writeLine("To run the program from the command-line:");
		writeLine("EN605202Lab4 {parameter0} {parameter1} {parameter2}");
		writeLine("");
		writeLine("Where:");
		writeLine("{parameter0} represents the sort type being run.");
		writeLine("{parameter1} is the filepath of the input file of elements to be sorted.");
		writeLine("{parameter2} is the filepath of the output file for the sort algorithms.");
		writeLine("");
		writeLine("{parameter0} options:");
		writeLine("- HeapSort -> Use a heap sort.");
		writeLine("- QuickSort -> Use a quick sort.");
		writeLine("- QuickSortInsertionSortHybrid50 -> Use a quick sort finishing off with an insertion sort for partition size below 50.");
		writeLine("- QuickSortInsertionSortHybrid100 -> Use a quick sort finishing off with an insertion sort for partition size below 100.");
		writeLine("- QuickSortMedianOfThree -> Use a quick sort with a median-of-three as pivot.");
	}
	
	/**
	 * Converts array of type Integer[] to int[].
	 * @author				Tom Atwood
	 * @version				1.0.0.0
	 * @since				1.0.0.0
	 * @param IntegerArray	Array to convert from type Integer[] to int[].
	 * @return				Returns the passed array in type int[].
	 */
	public static int[] toPrimitive(Integer[] IntegerArray) {

		int[] result = new int[IntegerArray.length];
		for (int i = 0; i < IntegerArray.length; i++) {
			result[i] = IntegerArray[i].intValue();
		}
		return result;
	}

	/**
	 * Provides a fresh screen to display validation messages.
	 * @author				Tom Atwood
	 * @version				1.0.0.0
	 * @since				1.0.0.0
	 * @param message		Message to be displayed.
	 */
	 private static void validationFailureScreen(String message) {
		clearScreen();
		writeLine("EN605.202.81 Data Structures, Spring 2017, Lab #4 - Informational Message");
		writeLine("");
		writeLine(message);
		writeLine("");
		writeLine("Please type 'EN605202Lab4 /? to get the help screen.");		
	}
	
	/**
	 * A method to simplify writing to the console.
	 * @author				Tom Atwood
	 * @version				1.0.0.0
	 * @since				1.0.0.0
	 * @param line			Line to be written to the screen.
	 */
	private static void writeLine(String line) {
		System.out.println(line);
	}
}