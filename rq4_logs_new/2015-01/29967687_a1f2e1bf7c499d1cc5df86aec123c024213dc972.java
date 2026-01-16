package Base;

import java.util.Scanner;

public class Main {

	public static void main(String[] args) {
		Tournament t = new Tournament();
		
		
		System.out.println("Combien d'�quipe ?");
		Scanner sc = new Scanner(System.in);
		String str = sc.nextLine();
		int nbr_equipe = Integer.parseInt(str);
		
		
		for (int i=0; i<nbr_equipe; i++){
			Team a = new Team(Integer.toString(i));
			t.add_team(a);
		}
		
		t.print_team();
		
		
		String input = "";
		while(input != "0"){
			input = sc.nextLine();
			switch(input) {
				case "help":
					break;
				case "0":
					System.out.println("Sortie du programme");
					input = "0";
					break;
					
				case "1":
					System.out.println("Modification des informations d'une team");
					System.out.println("Choix de la team � modif, les afficher ?(y/n)");
					String input2 = sc.nextLine();
					if (input2 == "y"){
						t.print_team();
					}
					System.out.println("Entrez le nom");
					String team_to_change = sc.nextLine();
					
					
					switch(input2) {
						case "help":
							break;
						case "1":
							System.out.println("Modification du nom");
							break;
						default:
							System.out.println("nop");
							break;
					
					
					}
					
					break;
				default:
					System.out.println("nop");
					break;
			}
		}
	}

}