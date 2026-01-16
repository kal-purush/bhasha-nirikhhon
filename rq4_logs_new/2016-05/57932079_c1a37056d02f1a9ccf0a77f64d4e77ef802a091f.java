package neu.mr.cs6240.a3_redux;

import java.util.ArrayList;

import neu.mr.cs6240.pseudo_cloud.AvgPricePerMonthDriver;

/**
 * @author ajay subramanya & smitha bangalore naresh
 * @date 02/05/2016
 * @info Assignment 3 Main class to start the program Command lines as specified
 *       below to choose appropriate run configuration and calculation
 */
public class App {
	/**
	 * Command Line Arguments <input path> <output path> <s | m | p | c> <e | d
	 * | f>
	 *
	 * <s | m | p | c> flags are described as below : s -> Run single threaded
	 * sequential program m -> Run multi threaded program p -> Run on pseudo
	 * distributed mode c -> Run on cloud
	 *
	 * <e | d | f> flags are described as below : e -> Calculate Mean d ->
	 * Calculate Median f -> Calculate Fast Median
	 *
	 */
	public static void main(String[] preArgs) throws Exception {
		String[] args = validateCLIArgs(preArgs);

		if (args.length != 4) {
			System.err.println(
					"Usage: -s3JarPath=<s3 Path of Jar> -input=<input path> -output=<output path> -calculate=<e | d | f>");
			System.exit(-1);
		}

		// Running the code in pseudo mode
		AvgPricePerMonthDriver driver = new AvgPricePerMonthDriver();
		System.out.println("Starting--Pseudo");
		driver.run(args);

	}

	/**
	 * validates the arguments coming to the code keeps only arguments passed in
	 * the format <-someName=someValue> ignores any other format
	 *
	 * @param preArgs
	 *            String array of all the arguments passed to the code
	 * @return args String array of only valid arguments
	 */
	private static String[] validateCLIArgs(String[] preArgs) {
		ArrayList<String> tempArgs = new ArrayList<>();
		for (int i = 0; i < preArgs.length; i++) {
			String[] options = preArgs[i].split("=");
			/**
			 * adding only if (1) argument starts with '-' (2) contains '=' (3)
			 * length of the split is more than 1, else for default options will
			 * throw ArrayIndexOutOfBoundsException
			 */
			if (preArgs[i].startsWith("-") && preArgs[i].contains("=") && options.length > 1) {
				tempArgs.add(options[1]);
			}
		}
		String[] args = new String[tempArgs.size()];
		args = tempArgs.toArray(args);
		return args;
	}
}