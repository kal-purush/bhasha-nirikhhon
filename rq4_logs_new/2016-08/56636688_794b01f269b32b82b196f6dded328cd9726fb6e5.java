import java.util.*;

class Solution {

	static ArrayList<String> deck = new ArrayList<String>();
	static ArrayList<String> wardeck1 = new ArrayList<String>();
	static ArrayList<String> wardeck2 = new ArrayList<String>();
	static ArrayList<String> first = new ArrayList<String>();
	static ArrayList<String> second = new ArrayList<String>();
	static int moves = 0;
	static boolean equal = false;

	public static void main(String args[]) {

		Scanner in = new Scanner(System.in);
		int n = in.nextInt(); // the number of cards for player 1
		for (int i = 0; i < n; i++) {
			String cardp1 = in.next(); // the n cards of player 1
			first.add(cardp1);
		}

		int m = in.nextInt(); // the number of cards for player 2
		for (int i = 0; i < m; i++) {
			String cardp2 = in.next(); // the m cards of player 2
			second.add(cardp2);
		}

		while (!first.isEmpty() && !second.isEmpty()) {

			System.err.println("First: " + first);
			System.err.println("Second: " + second);

			String f = first.get(0).substring(0, 1);
			String s = second.get(0).substring(0, 1);

			// if first aren't equal:
			if (!f.equals(s)) {
				System.err.println("First arent equal");

				deck.add(first.get(0));
				deck.add(second.get(0));

				System.err.println("Deck: " + deck);

				if (isBiggerThen(f, s) == true) {
					first.addAll(deck);
				}

				else if (isBiggerThen(s, f) == true) {
					second.addAll(deck);
				}

				// cleaning up:
				first.remove(0);
				second.remove(0);
				deck.clear();

				moves++;
				System.err.println("MOVE ADDED BECAUSE IT WAS: " + moves);
			}

			// if first are equal, then war begins
			else if (f.equals(s)) {
				System.err.println("War has been started");
				equal = true;

				// check if cards are enough:
				if (first.size() >= 4 && second.size() >= 4) {
					System.err.println("Cards are enough");
				}

				else if (first.size() < 4 || second.size() < 4) {
					System.err.println("Not enough cards");

					System.out.println("PAT");
				}

				// while cards are enough and first are equal
				while (first.size() >= 4 && second.size() >= 4 && equal == true) {

					// adding element to decks:
					for (int i = 0; i < 4; i++) {
						wardeck1.add(first.get(i));
					}
					for (int i = 0; i < 4; i++) {
						wardeck2.add(second.get(i));
					}

					// removing elements from players
					for (int i = 0; i < 4; i++) {
						first.remove(0);
					}
					for (int i = 0; i < 4; i++) {
						second.remove(0);
					}

					System.err.println("First: " + first);
					System.err.println("Second: " + second);
					System.err.println("Wardeck: " + wardeck1 + wardeck2);

					String k1 = first.get(0).substring(0, 1);
					String k2 = second.get(0).substring(0, 1);

					if (!k1.equals(k2)) {
						equal = false;
						System.err.println("EQUAL is now false");
					}

					System.err.println(k1);
					System.err.println(k2);
				}

				System.err.println("War cycle is over");

				// comparing first elements

				String f1 = first.get(0).substring(0, 1);
				String s1 = second.get(0).substring(0, 1);

				if (isBiggerThen(f1, s1) == true) {

					first.addAll(wardeck1);
					first.add(first.get(0));
					first.addAll(wardeck2);
					first.add(second.get(0));

				}

				else if (isBiggerThen(s1, f1) == true) {

					second.addAll(wardeck1);
					second.add(first.get(0));
					second.addAll(wardeck2);
					second.add(second.get(0));

				}

				// clearing up
				first.remove(0);
				second.remove(0);
				wardeck1.clear();
				wardeck2.clear();

				System.err.println("War has been finished");

				moves++;
				System.err.println("MOVE ADDED BECAUSE WAR FINISHED: " + moves);
			}

		}

		if (first.isEmpty() && second.isEmpty()) {
			System.out.println("PAT");
		}

		else if (first.isEmpty()) {
			System.out.println("2 " + moves);
		}

		else if (second.isEmpty()) {
			System.out.println("1 " + moves);
		}
	}

	private static boolean isBiggerThen(String f, String t) {
		String f1 = f.substring(0, 1);
		String t1 = t.substring(0, 1);

		int fint;
		int tint;

		switch (f1) {
		case "2":
			fint = 2;
			break;
		case "3":
			fint = 3;
			break;
		case "4":
			fint = 4;
			break;
		case "5":
			fint = 5;
			break;
		case "6":
			fint = 6;
			break;
		case "7":
			fint = 7;
			break;
		case "8":
			fint = 8;
			break;
		case "9":
			fint = 9;
			break;
		case "1":
			fint = 10;
			break;
		case "J":
			fint = 11;
			break;
		case "Q":
			fint = 12;
			break;
		case "K":
			fint = 13;
			break;
		case "A":
			fint = 14;
			break;
		default:
			fint = 2;
			break;
		}

		switch (t1) {
		case "2":
			tint = 2;
			break;
		case "3":
			tint = 3;
			break;
		case "4":
			tint = 4;
			break;
		case "5":
			tint = 5;
			break;
		case "6":
			tint = 6;
			break;
		case "7":
			tint = 7;
			break;
		case "8":
			tint = 8;
			break;
		case "9":
			tint = 9;
			break;
		case "1":
			tint = 10;
			break;
		case "J":
			tint = 11;
			break;
		case "Q":
			tint = 12;
			break;
		case "K":
			tint = 13;
			break;
		case "A":
			tint = 14;
			break;
		default:
			tint = 2;
			break;
		}

		if (fint > tint) {
			return true;
		}

		else {
			return false;
		}
	}
}