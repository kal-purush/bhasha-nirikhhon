package bigram_Implementation;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Scanner;
import java.util.TreeMap;

/**
 * 
 */

/**
 * @author Anantha
 *
 */
public class NBTest {

	// ApplyMultonimialNB(C, V, prior, condprob, d) d is the testDocument
	// W - Extract tokens from doc

	public static double applyMultinomialNB(HashMap<String, HashMap<Reviews, Double>> vocabulary, File file, double priorValue, Reviews review){
		double score = 0;

		// method to calculate score to classify the given files 
		// score += log condProb[token][class] + log prior[class]

		Scanner scanner;
		try{

			scanner = new Scanner(file);
			while(scanner.hasNextLine()){
				String term = "";
				int count = 0;
				String line1 = scanner.nextLine().replaceAll("&nbsp", ""); 
				String[] line = line1.split("\\s+");
				for(int i=0; i<line.length; i++){
					term += line[i] + " ";
					count++;
					if(count == 2){
						term.trim();
					if(vocabulary.containsKey(term)){
						score += Math.log(vocabulary.get(term).get(review));
					}
					term = "";
					count = 0;
				}
				}
			}

		}
		catch(Exception e){
			e.printStackTrace();
		}

		score += Math.log(priorValue);



		return score;

	}

	public static void main(String[] args) throws IOException {

		NBTrain train = new NBTrain();

		System.out
		.println("Enter the FULL path of files to be trained with negative files path first"
				+ " and positive files path second comma seperated(/Users/path/train/neg,/Users/path/train/pos): ");

		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String s = br.readLine();

		System.out.println("Enter the FULL path of files to be tested: ");
		String testPath = br.readLine();

		HashMap<String, Object> result = train.trainMultinomialNB(s);

		HashMap<String, HashMap<Reviews, Double>> vocabulary = (HashMap<String, HashMap<Reviews, Double>>)result.get("Vocabulary");
		System.out.println(vocabulary.size() + " train value");
		System.out.println(((TreeMap<Double, TreeMap<String,String>>)result.get("PositiveToNegative")).size() + " posToNeg value");
		System.out.println(((TreeMap<Double, TreeMap<String,String>>)result.get("NegativeToPositive")).size() + " negToPos value");

		double priorPositive = (double)result.get("PriorPositive");
		double priorNegative = (double)result.get("PriorNegative");

		File dir = new File(testPath);
		int cnt = 0;
		for (File file : dir.listFiles()) {
			double scorePositive = applyMultinomialNB(vocabulary, file, priorPositive, Reviews.POSITIVE);
			double scoreNegative = applyMultinomialNB(vocabulary, file, priorNegative, Reviews.NEGATIVE);
			String status = scorePositive > scoreNegative ? "Positive" : "Negative";
			cnt += status.equals("Positive") ? 1 : 0;
			System.out.println(file + " " + scoreNegative + " " + scorePositive + " " + status);
		}

		System.out.println(cnt);
	}
}

