/**
 * Copyright 2015-2016 Debmalya Jash
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hr.WorldCodeSprint9;

import java.util.Scanner;
import java.util.Stack;

/**
 * @author debmalyajash
 *
 */
public class BoxOperations {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try (Scanner in = new Scanner(System.in)) {
			int noOfBoxes = in.nextInt();
			int noOfOperations = in.nextInt();

			int[] boxes = new int[noOfBoxes];

			for (int i = 0; i < noOfBoxes; i++) {
				boxes[i] = in.nextInt();
			}

			in.nextLine();
			Stack<Integer> operations = new Stack<>();
			Stack<Double> operands = new Stack<>();
			Stack<Integer> startIndex = new Stack<>();
			Stack<Integer> endIndex = new Stack<>();
			
			for (int i = 0; i < noOfOperations; i++) {
				String operation = in.nextLine();
				String[] commands = operation.split(" ");
				int start = Integer.parseInt(commands[1]);
				int end = Integer.parseInt(commands[2]);
				double operand = Double.parseDouble(commands[3]);
				operands.add(operand);
				startIndex.add(start);
				
				if (commands[0].equals("1")) {				
					operations.add(1);
					
				} else if (commands[0].equals("2")) {
					operations.add(2);
				} else if (commands[0].equals("3")) {
					
					int min = Integer.MAX_VALUE;
					for (int j = start; j <= end; j++) {
						min = Math.min(min, boxes[j]);
					}
					System.out.println(min);
				} else if (commands[0].equals("4")) {
					long sum = 0;
					for (int j = start; j <= end; j++) {
						sum += boxes[j];
					}
					System.out.println(sum);
				}
			}
		}

	}

	/**
	 * @param args
	 */
	public static void main0(String[] args) {
		try (Scanner in = new Scanner(System.in)) {
			int noOfBoxes = in.nextInt();
			int noOfOperations = in.nextInt();

			int[] boxes = new int[noOfBoxes];

			for (int i = 0; i < noOfBoxes; i++) {
				boxes[i] = in.nextInt();
			}

			in.nextLine();
			for (int i = 0; i < noOfOperations; i++) {
				String operation = in.nextLine();
				String[] commands = operation.split(" ");
				if (commands[0].equals("1")) {
					int start = Integer.parseInt(commands[1]);
					int end = Integer.parseInt(commands[2]);
					int add = Integer.parseInt(commands[3]);
					for (int j = start; j <= end; j++) {
						boxes[j] += add;
					}
				} else if (commands[0].equals("2")) {
					int start = Integer.parseInt(commands[1]);
					int end = Integer.parseInt(commands[2]);
					double div = Double.parseDouble(commands[3]);
					for (int j = start; j <= end; j++) {
						boxes[j] = (int) Math.floor((double)boxes[j]
								/ div);
					}
				} else if (commands[0].equals("3")) {
					int start = Integer.parseInt(commands[1]);
					int end = Integer.parseInt(commands[2]);
					int min = Integer.MAX_VALUE;
					for (int j = start; j <= end; j++) {
						min = Math.min(min, boxes[j]);
					}
					System.out.println(min);
				} else if (commands[0].equals("4")) {
					int start = Integer.parseInt(commands[1]);
					int end = Integer.parseInt(commands[2]);
					long sum = 0;
					for (int j = start; j <= end; j++) {
						sum += boxes[j];
					}
					System.out.println(sum);
				}
			}
		}

	}
}