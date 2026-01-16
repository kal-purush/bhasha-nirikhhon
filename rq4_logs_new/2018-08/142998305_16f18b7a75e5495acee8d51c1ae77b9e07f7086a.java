package com.ymagis.daf;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.ymagis.daf.response.StartResponse;
import com.ymagis.daf.response.TestResponse;


public class App {
	private Game game;
	private static boolean testLocal = true;
	private static int callCount = 0;

	public final static String RIGHT = "12345";

	public App() {
		game = new Game();
	}

	public static void main(String[] args) {
		App app = new App();
		int N = 5;
		if (!testLocal) {
			StartResponse startResponse = app.game.start();
			System.out.println(startResponse);
			N = startResponse.getSize();
		}

		int[] ret = app.masterMind(N);
		Set<Integer> alreadyPlaced = new HashSet<Integer>();
		int[] test = new int[N];
		app.callTest(N, 0, alreadyPlaced, test, ret);
		System.out.println(Arrays.toString(ret));
		System.out.println(Arrays.toString(test));
	}

	public int[] masterMind(int n) {
		String number = "";
		int count = 0;
		int[] ret = new int[n];
		for (int i = 0; i < 10; i++) {
			number = "";
			for (int j = 0; j < n; j++) {
				number += i;
			}
			TestResponse testResponse = null;
			if (testLocal) {
				testResponse = check(number, RIGHT);
			} else {
				testResponse = game.test(number);
			}
			if (testResponse.getGood() > 0) {
				for (int j = 0; j < testResponse.getGood(); j++) {
					ret[count] = i;
					count++;
				}
			}
			if (count == n) {
				break;
			}
		}
		return ret;
	}

	private String intToString(int[] number) {
		StringBuffer string = new StringBuffer();
		for (int i : number) {
			string.append(i + "");
		}
		return string.toString();
	}

	public void callTest(int N, int idx, Set<Integer> alreadyPlaced, int[] test, int[] tableNumber) {
		for (int i = 0; i < N; ++i) {
			if (alreadyPlaced.contains(i))
				// if i in alreadyPlaced
				continue;

			alreadyPlaced.add(i);
			test[idx] = tableNumber[i];
			TestResponse testResponse = null;
			if (idx == (N - 1)) {

				String answer = intToString(test);
				if (RIGHT.equals(answer)) {
					System.out.println("good answer: found");
					System.out.println(Arrays.toString(test));
					System.out.println("call count : " + callCount);
					System.exit(0);
				} else {
					if (testLocal) {
						testResponse = check(answer, RIGHT); //
					} else {
						testResponse = game.test(intToString(test)); //
					}
				}

				if (testResponse.getGood() == N) {
					System.out.println("good answer");
					System.out.println(Arrays.toString(test));
					System.out.println("call count : " + callCount);
					System.exit(0);
				}
				System.out.println(Arrays.toString(test));
				System.out.println(testResponse);

			} else
				callTest(N, idx + 1, alreadyPlaced, test, tableNumber);

			alreadyPlaced.remove(i);
		}
	}

	public TestResponse check(String result, String rightAnswer) {
		// si
		callCount++;
		TestResponse response = new TestResponse();
		char[] resultArray = result.toCharArray();
		char[] rightAnswerArray = rightAnswer.toCharArray();
		int size = resultArray.length;
		int good = 0;
		int wrong_place = 0;
		for (int i = 0; i < size; i++) {
			if (resultArray[i] == rightAnswerArray[i]) {
				good++;
			}
		}
		response.setGood(good);
		response.setWrongPlace(wrong_place);
		return response;
	}
}