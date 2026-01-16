package com.camcast.contest.taxperson;

public class ApplyTax {

	static long amount = 0;

	/**
	 * Finds an item is luxury or necessity from a double dimensional array and
	 * also able to calculate the quantity based on the price.
	 * 
	 * @param items
	 * @param item
	 * @param qty
	 * @return
	 */

	public long findTax(String[][] items, String item, int qty) {

		for (String i[] : items) {

			if (i[0].equalsIgnoreCase(item)) {

				amount = (Integer.parseInt(i[1]) * qty);

				if (i[2].equalsIgnoreCase("N")) {

					System.out.println("Necessity");

					amount += amount * 0.01;

				}

				else if (i[2].equalsIgnoreCase("L")) {

					System.out.println("Luxury");

					amount += amount * 0.09;

				}

			}

		}

		System.out.println("Cost of Item(s) including Tax : " + amount);

		return amount;
	}

}