package gameOn;
import java.util.Scanner;
public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Scanner sc = new Scanner (System.in);
		
		PlayerStatus player1 = new PlayerStatus();
		PlayerStatus player2 = new PlayerStatus();
		PlayerStatus player3 = new PlayerStatus();
		
		player1.initPlayer("Naix", 5, 0);
		player2.initPlayer("JohnDoe", 5, 0);
		player3.initPlayer("Storm", 5, 0, 78);
		player1.findArtifact(28);
		player2.findArtifact(6);
		player2.findArtifact(6);
		player1.findArtifact(13);
		player1.findArtifact(28);
		player1.findArtifact(28);
		player1.findArtifact(28);
		player2.findArtifact(6);
		player2.findArtifact(6);
		player2.findArtifact(6);
		player3.findArtifact(13);
		System.out.println("health: "+player3.getHealth());
		
		System.out.println("lives: "+player1.getLives());
	
		//player1.getWeaponInHand();
		player1.setPositionX(2);
		player2.setPositionX(1);
		player1.setPositionY(4);
		player2.setPositionY(2);
		//System.out.println(player1.score);
		player1.setWeaponInHand("sniper");
		System.out.println(player1.getWeaponInHand());
		
		System.out.println("distance: "+player1.distance(player2));
		//System.out.println(player1.score);
		//System.out.println(player1.weaponInHand);
		
		player2.setWeaponInHand("sniper");
		
		System.out.println(player2.getWeaponInHand());
		player1.shouldAttackOpponent(player2);
		player2.diplayFields();
	}

}