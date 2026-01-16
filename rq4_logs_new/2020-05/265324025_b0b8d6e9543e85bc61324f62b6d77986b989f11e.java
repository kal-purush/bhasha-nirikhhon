
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

//Example invocations of the program:
//1. To print out the probability P(Burglary=true and Alarm=false | MaryCalls=false).
//         bnet Bt Af given Mf
//2. To print out the probability P(Alarm=false and Earthquake=true).
//    bnet Af Et
//3. To print out the probability P(JohnCalls=true and Alarm=false | Burglary=true and Earthquake=false).
//     bnet Jt Af given Bt Ef
//4. To print out the probability P(Burglary=true and Alarm=false and MaryCalls=false and JohnCalls=true and Earthquake=true).
//     bnet Bt Af Mf Jt Et

// calcProb = 5.938020000000005E-5
// denoVal = 0.9882636550199999
// conditions = {A=[F], B=[T], E=[T, F], J=[T, F], M=[F]}
//{A=[F], B=[T], E=[T, F], J=[T, F], M=[F]}
// givenValues = {A=[T, F], B=[T, F], E=[T, F], J=[T, F], M=[F]}
//{A=[T, F], B=[T, F], E=[T, F], J=[T, F], M=[F]}
/**
 * @author Divya Saxena
 *
 */
public class BayeisnNetwork {

	private static final String EXIT_MESSAGE_6 = "The number of arguments should be between 1 - 6.\\n!!!Exiting the Program!!!!";
	private static final String EXIT_MESSAGE_5 = "The number of arguments should be between 1 - 5.\\n!!!Exiting the Program!!!!";
	private static final String EXIT_MESSAGE_4 = "The number of arguments should be between 1 - 4.\\n!!!Exiting the Program!!!!";
	private static Map<String, Double> BURGLARY_TT = new HashMap<>();
	private static Map<String, Double> EARTHQUAKE_TT = new HashMap<>();
	private static Map<String, Double> ALARM_TT = new HashMap<>();
	private static Map<String, Double> JOHN_CALL_TT = new HashMap<>();
	private static Map<String, Double> MARY_CALL_TT = new HashMap<>();

	public void calculateBayeisnNetwork(String[] args) {
		// TODO Auto-generated method stub
		// Enter data using BufferReader
//		0.00006008538278056816
//		String[] stringArray = new String[] { "bnet", "Bt", "Af", "given", "Mf" };
//		String[] stringArray = new String[] { "bnet", "Af", "Et" };
//		0.00300000000000000270
//		String[] stringArray = new String[]{"bnet", "Jt", "Af", "given", "Bt", "Ef"};
//		0.00000000495000000000
//		String[] stringArray = new String[]{"bnet", "Bt", "Af", "Mf", "Jt", "Et"};

		int counter = 0;

		Map<Character, ArrayList<Character>> givenValues = new HashMap<>();
		Map<Character, ArrayList<Character>> conditions = new HashMap<>();
		checkNoOfArguments(args);
		loadValues();
		for (String inline : args) {
			if (inline.equals("given")) {
				counter = 1;
				continue;
			}
			if (counter == 0) {
				conditions = filterValues(inline, conditions);
			} else {
				givenValues = filterValues(inline, givenValues);
			}
		}
		checkSizeOfConditions(conditions);
		checkSizeOfGivenValues(counter, givenValues);
		conditions.putAll(givenValues);
		conditions.putAll(generateRemainingPossibleValues(conditions));
		givenValues.putAll(generateRemainingPossibleValues(givenValues));
		double calcProb = calculateProbability(conditions);
		if (counter == 1) {
			double denominatorValue = calculateProbability(givenValues);
			calcProb = calcProb / denominatorValue;

		}
		printProbabilityOfStatement(args, calcProb);
	}

	private Map<Character, ArrayList<Character>> filterValues(String inline,
			Map<Character, ArrayList<Character>> mapValues) {
		ArrayList<Character> possibleBool = new ArrayList<>();
		String temp = inline.toUpperCase();
		possibleBool.add(temp.charAt(1));
		mapValues.put(temp.charAt(0), possibleBool);
		return mapValues;
	}

	private void checkNoOfArguments(String[] args) {
		if (args.length < 1 || args.length > 6) {
			System.out.println(EXIT_MESSAGE_6);
			System.exit(0);
		}
	}

	private void checkSizeOfConditions(Map<Character, ArrayList<Character>> conditions) {
		if (conditions.keySet().size() < 1 || conditions.keySet().size() > 5) {
			System.out.println(EXIT_MESSAGE_5);
			System.exit(0);
		}
	}

	private void checkSizeOfGivenValues(int counter, Map<Character, ArrayList<Character>> givenValues) {
		if (counter == 1 && givenValues.keySet().size() < 1 || givenValues.keySet().size() > 4) {
			System.out.println(EXIT_MESSAGE_4);
			System.exit(0);
		}
	}

	private void printProbabilityOfStatement(String[] args, double calcProb) {
		System.out.print("Probability of the statement ' ");
		for (int s = 0; s < args.length; s++) {
			System.out.print(args[s] + " ");
		}
		System.out.printf("' = %.20f\n", calcProb);
	}

	private Map<Character, ArrayList<Character>> generateRemainingPossibleValues(
			Map<Character, ArrayList<Character>> passedMap) {
		Map<Character, ArrayList<Character>> possibleVal = new HashMap<Character, ArrayList<Character>>();
		possibleVal = checkForKey('B', possibleVal, passedMap);
		possibleVal = checkForKey('E', possibleVal, passedMap);
		possibleVal = checkForKey('A', possibleVal, passedMap);
		possibleVal = checkForKey('J', possibleVal, passedMap);
		possibleVal = checkForKey('M', possibleVal, passedMap);
		return possibleVal;
	}

	private void loadValues() {
		loadBurglary();
		loadEarthQuake();
		loadAlarm();
		loadJohnCall();
		loadMaryCall();
	}

	private void loadBurglary() {
		BURGLARY_TT.put("B_T", 0.001);
	}

	private void loadEarthQuake() {
		EARTHQUAKE_TT.put("E_T", .002);
	}

	private void loadAlarm() {
		ALARM_TT.put("A_T|B_T,E_T", 0.95);
		ALARM_TT.put("A_T|B_T,E_F", 0.94);
		ALARM_TT.put("A_T|B_F,E_T", 0.29);
		ALARM_TT.put("A_T|B_F,E_F", 0.001);
	}

	private void loadJohnCall() {
		JOHN_CALL_TT.put("J_T|A_T", 0.90);
		JOHN_CALL_TT.put("J_T|A_F", 0.05);
	}

	private void loadMaryCall() {
		MARY_CALL_TT.put("M_T|A_T", 0.70);
		MARY_CALL_TT.put("M_T|A_F", 0.01);
	}

	private double calculateProbability(Map<Character, ArrayList<Character>> values) {
		double probabilityValue = 0.0;
		for (int burglaryCount = 0; burglaryCount < values.get('B').size(); burglaryCount++) {

			for (int earthquakeCount = 0; earthquakeCount < values.get('E').size(); earthquakeCount++) {

				for (int alarmCount = 0; alarmCount < values.get('A').size(); alarmCount++) {

					for (int johnCallCount = 0; johnCallCount < values.get('J').size(); johnCallCount++) {

						for (int maryCallCount = 0; maryCallCount < values.get('M').size(); maryCallCount++) {

							probabilityValue += computeProbability(values.get('B').get(burglaryCount),
									values.get('E').get(earthquakeCount), values.get('A').get(alarmCount),
									values.get('J').get(johnCallCount), values.get('M').get(maryCallCount));

						}
					}
				}
			}
		}
		return probabilityValue;
	}

	private double computeProbability(char burglary, char earthquake, char alarm, char johnCall, char maryCall) {
		double computedProbability = 0.0;

		double burglaryVal = calculateForF(burglary, BURGLARY_TT.get("B_T"));
		double earthquakeVal = calculateForF(earthquake, EARTHQUAKE_TT.get("E_T"));
		double alarmVal = calculateForF(alarm, ALARM_TT.get("A_T|B_" + burglary + ",E_" + earthquake));
		double johnCallVal = calculateForF(johnCall, JOHN_CALL_TT.get("J_T|A_" + alarm));
		double maryCallVal = calculateForF(maryCall, MARY_CALL_TT.get("M_T|A_" + alarm));

		computedProbability = burglaryVal * earthquakeVal * alarmVal * johnCallVal * maryCallVal;
		return computedProbability;
	}

	private double calculateForF(char characterValue, double doubleValue) {
		double fValue = 0.0;
		if (characterValue == 'F') {
			fValue = 1.00 - doubleValue;
		} else if (doubleValue > 0) {
			fValue = doubleValue;
		}
		return fValue;
	}

	private Map<Character, ArrayList<Character>> checkForKey(char key, Map<Character, ArrayList<Character>> possibleVal,
			Map<Character, ArrayList<Character>> passedMap) {
		if (!(passedMap.containsKey(key))) {
			ArrayList<Character> possibleBool = new ArrayList<>();
			possibleBool.add('T');
			possibleBool.add('F');
			possibleVal.put(key, possibleBool);
		}
		return possibleVal;
	}

}