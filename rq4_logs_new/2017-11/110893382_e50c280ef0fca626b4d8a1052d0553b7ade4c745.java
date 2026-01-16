package textMancala;

import java.util.*;

public class Mancala {

	private int stones;
	private int playerBS;
	private int playerAS;
	private boolean playerATurn;

	private ArrayList<Pit> pList; // 0-5 is a1 to a6, 6-11 is b1-b6
	private Pit aPit;
	private Pit bPit;
	private int end;
	Scanner myScan = new Scanner(System.in);

	public Mancala(int s) {

		stones = s;
		playerAS = 0;
		playerBS = 0;
		playerATurn = true; // True for player A, false for player B
		pList = new ArrayList<Pit>();
		for (int i = 0; i < 12; i++) {

			pList.add(new Pit(stones));

		}
		aPit = new Pit(0);
		bPit = new Pit(0);
		end = 0; // 0 if game hasn't ended, 1 or any other value if it has

	}

	public void playGame() {

		while (end == 0) {

			playTurn();

		}
		boolean aWin = aPit.returnStones() > bPit.returnStones();
		if(aWin) {
			
			System.out.println("Player A wins!");
			
		}
		else {
			
			System.out.println("Player B wins!");
			
		}
		
	}

	public void playTurn() {

		if (playerATurn) {

			System.out.println("Player A's Turn");

		} else {

			System.out.println("Player B's Turn");

		}

		System.out.println("Choose a pit");
		while (!myScan.hasNextInt()) 
			myScan.next();
			
		int pitN = myScan.nextInt();
		
		
		if (!playerATurn) {

			pitN = pitN + 6;

		}
		int pitNIndex = pitN - 1;
		int stoneInPit = pList.get(pitNIndex).emptyPit();
		if (playerATurn) {

			int startI = pitNIndex;
			int endI = pitNIndex + stoneInPit;
			if (startI <= 5 && endI >= 6) {

				endI = endI - 1;
				aPit.addStone(1);

			}
			for (int index = startI + 1; index < endI + 1; index++) {

				int tIndex = index;
				if (index > 11) {

					tIndex = index % 12;

				}

				if (index == endI && onPlayerSide(tIndex) && pList.get(tIndex).returnStones() == 0) {

					aPit.addStone(1 + pList.get(11 - tIndex).emptyPit());

				} else {
					pList.get(tIndex).addStone(1);
				}
			}

		} else {

			int startI = pitNIndex;
			int endI = pitNIndex + stoneInPit;
			if (startI <= 11 && endI >= 12) {

				endI = endI - 1;
				bPit.addStone(1);

			}
			for (int index = startI + 1; index < endI + 1; index++) {

				int tIndex = index;
				if (index > 11) {

					tIndex = index % 12;

				}
				if (index == endI && onPlayerSide(tIndex) && pList.get(tIndex).returnStones() == 0) {

					bPit.addStone(1 + pList.get(11 - tIndex).emptyPit());

				} else {
					pList.get(tIndex).addStone(1);
				}
			}
		}

		boolean end2 = testEnd();
		if(end2) {
			
			end = 1;
			
		}
		playerATurn = !playerATurn;
		printBoard();
	}

	public boolean testEnd() {

		int aEnd = 0;
		for (int i = 0; i < 6; i++) {

			if (pList.get(i).returnStones() == 0) {

				aEnd++;

			}
		}
		if (aEnd == 6) {

			return true;

		}
		int bEnd = 0;
		for (int i = 6; i < 12; i++) {

			if (pList.get(i).returnStones() == 0) {

				bEnd++;

			}
		}
		if(bEnd == 6) {
			
			return true;
			
		}
		return false;
	}

	public boolean onPlayerSide(int index) {

		if (playerATurn) {

			if (index <= 5) {

				return true;

			} else {

				return false;

			}
		} else {

			if (index <= 11 && index >= 6) {

				return true;

			} else {

				return false;

			}

		}

	}

	public Pit getPit(int index) {

		if (index > 12 || index < 0) {

			System.out.println("Out of bounds");
			return new Pit(0);
		}
		return pList.get(index);
	}

	public void printStartBoard() {

		System.out.println("   " + " " + stones + " " + " " + stones + " " + " " + stones + " " + " " + stones + " "
				+ " " + stones + " " + " " + stones + " ");
		System.out.println(" " + playerAS + " " + "                  " + " " + playerBS + " ");
		System.out.println("   " + " " + stones + " " + " " + stones + " " + " " + stones + " " + " " + stones + " "
				+ " " + stones + " " + " " + stones + " ");

	}

	public void printBoard() {

		System.out.print("   ");
		for (int i = 11; i > 5; i--) {

			System.out.print(" " + getPit(i).returnStones() + " ");

		}
		System.out.println();
		System.out.println(" " + bPit.returnStones() + " " + "                  " + " " + aPit.returnStones() + " ");
		System.out.print("   ");
		for (int i = 0; i < 6; i++) {

			System.out.print(" " + getPit(i).returnStones() + " ");

		}
		System.out.println();

	}

	public static void main(String[] args) {

		Mancala m = new Mancala(3);
		m.printBoard();
		m.playGame();

	}

}