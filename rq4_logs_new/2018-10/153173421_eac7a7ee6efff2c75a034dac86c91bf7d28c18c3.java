package testingStuffBecause;

import java.util.Scanner;

public class BetterGame {
	
	static Scanner input = new Scanner(System.in);
	
	private Room[][] rooms = new Room[3][3];
	private Player player;
	
	private int currColumn = 0;
	private int currRow = 0;
	
	BetterGame(Player player) {
		this.player = player;
		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 3; j++ ) {
				if (i == 0 && j == 0) {
					rooms[0][0] = new Room(true);
				} else {
					rooms[i][j] = new Room(false);
				}
			}
		}
	}
	
	public void playGame() {
		boolean runGame = true;
		while (runGame == true) {
			player.setRoom(rooms[0][0]);
			System.out.println("You are playing the new game.");
			System.out.println("Type in North, South, East, or West to move.");
			
			boolean whichDirection = true;
			
			while(whichDirection) {
				System.out.println("You are column " + currColumn + " and row " + currRow);
				System.out.print("You can go ");
				if (currRow == 0) {
					System.out.print("South");
				}else if (currRow == 1) {
					System.out.print("North or South");
				}else {
					System.out.print("North");
				}
				if (currColumn == 0) {
					System.out.println(" or East");
				}else if (currColumn == 1) {
					System.out.println(" or East or West");
				}else {
					System.out.println(" or West");
				}
				String direction = input.nextLine();
				direction = direction.toLowerCase();
				if (direction.equals("north")) {
					if (currRow != 0) {
						System.out.println("You go North.");
						player.setRoom(rooms[currRow--][currColumn]);
						roomItem(player);
					} else {
						System.out.println("You can't move North");
					}
				} else if (direction.equals("south")) {
					if (currRow != 2) {
						System.out.println("You go South.");
						player.setRoom(rooms[currRow++][currColumn]);
						roomItem(player);
					} else {
						System.out.println("You can't move South");
					}
				} else if (direction.equals("east")) {
					if (currColumn != 2) {
						System.out.println("You go East.");
						player.setRoom(rooms[currRow][currColumn++]);
						roomItem(player);
					} else {
						System.out.println("You can't move East");
					}
				} else if (direction.equals("west")) {
					if (currColumn != 0) {
						System.out.println("You go West.");
						player.setRoom(rooms[currRow][currColumn--]);
						roomItem(player);
					} else {
						System.out.println("You can't move West");
					}
				} else if (direction.equals("stats")) {
					player.printStats(0);
					player.printInventory();
				}else {
					System.out.println("Not an option.");
				}
				
			}		
		}
	}
	public static void roomItem(Player player) {
		
		Room room = player.getRoom();
		
		Equipment theItem = room.getItem();
		
		System.out.println("You found a " + theItem.getName() + " Description: " + theItem.getDesc());
		System.out.println("It costs " + theItem.getPrice() + " money. You have " + player.getMoney() + " money.");
		
		boolean choice1 = true;
		
		while (choice1) {
			System.out.println("Do you want to buy it? (yes/no)");
			String pickUp = input.nextLine();
			pickUp = pickUp.toLowerCase();
			
			if (pickUp.equals("yes")) {
				if (!player.addItem(theItem)) {
					System.out.println("You are out of inventory space.");
				}
				choice1 = false;
			} else if (pickUp.equals("no")) {
				choice1 = false;
			} else {
				System.out.println("Not an option.");
			}
		}
	}
}