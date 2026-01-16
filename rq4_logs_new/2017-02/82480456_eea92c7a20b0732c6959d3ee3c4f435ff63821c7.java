import java.util.*;

public class GamePlay {

	
	
	public static Scanner scan = new Scanner(System.in);
	public static boolean gameOver = false;
	public static Stack<Disk> peg_A = new Stack<Disk>();
	public static Stack<Disk> peg_B = new Stack<Disk>();
	public static Stack<Disk> peg_C = new Stack<Disk>();
	public static int steps = 0;
	
	
	public static void main(String[] args) {
		
		
		
		Disk d = new Disk();
		
		int nop = 3;
		int nod = 3;
		
		
		
	
		
		d = new Disk(3,"D3");
		peg_A.push(d);
		
		d = new Disk(2,"D2");
		peg_A.push(d);
		
		d = new Disk(1,"D1");
		peg_A.push(d);
		
		while(!gameOver){
			displayBoard();
			getMove();
			checkGameOver();
		}
		
		System.out.println("Game Over in "+steps+" Steps!");
		
		
		

	}
	
	static void getMove(){
		
		int nd, np;
		Disk cd = null;
		
		System.out.println("");
		System.out.println("");
		
		System.out.print("Select Disk: ");
		nd = scan.nextInt();
		System.out.print("Select Peg: ");
		np = scan.nextInt();
		
		
			
			if(!peg_A.isEmpty() && peg_A.peek().getWeight()==nd){
				
				switch(np){
					case 1:
						if(peg_A.isEmpty()){
							cd = peg_A.pop();
							peg_A.push(cd);
						}
						else
						if(peg_A.peek().getWeight()>nd){
							cd = peg_A.pop();
							peg_A.push(cd);
						}
						else{
							System.out.print("Sorry Move not Possible");
							return;
						}
					break;
					case 2:
						if(peg_B.isEmpty()){
							cd = peg_A.pop();
							peg_B.push(cd);
						}
						else
						if(peg_B.peek().getWeight()>nd){
							cd = peg_A.pop();
							peg_B.push(cd);
						}
						else{
							System.out.print("Sorry Move not Possible");
							return;
						}
					break;
					case 3:
						if(peg_C.isEmpty()){
							cd = peg_A.pop();
							peg_C.push(cd);
						}
						else
						if(peg_C.peek().getWeight()>nd){
							cd = peg_A.pop();
							peg_C.push(cd);
						}
						else{
							System.out.print("Sorry Move not Possible");
							return;
						}
					break;
				}
				
			}
			else if(!peg_B.isEmpty() && peg_B.peek().getWeight()==nd){
				switch(np){
				case 1:
					if(peg_A.isEmpty()){
						cd = peg_B.pop();
						peg_A.push(cd);
					}
					else
					if(peg_A.peek().getWeight()>nd){
						cd = peg_B.pop();
						peg_A.push(cd);
					}
					else{
						System.out.print("Sorry Move not Possible");
						return;
					}
				break;
				case 2:
					if(peg_B.isEmpty()){
						cd = peg_B.pop();
						peg_B.push(cd);
					}
					else
					if(peg_B.peek().getWeight()>nd){
						cd = peg_B.pop();
						peg_B.push(cd);
					}
					else{
						System.out.print("Sorry Move not Possible");
						return;
					}
				break;
				case 3:
					if(peg_C.isEmpty()){
						cd = peg_B.pop();
						peg_C.push(cd);
					}
					else
					if(peg_C.peek().getWeight()>nd){
						cd = peg_B.pop();
						peg_C.push(cd);
					}
					else{
						System.out.print("Sorry Move not Possible");
						return;
					}
				break;
			}
				
			}
			else if (!peg_C.isEmpty() && peg_C.peek().getWeight()==nd){
				switch(np){
				case 1:
					if(peg_A.isEmpty()){
						cd = peg_C.pop();
						peg_A.push(cd);
					}
					else
					if(peg_A.peek().getWeight()>nd){
						cd = peg_C.pop();
						peg_A.push(cd);
					}
					else{
						System.out.print("Sorry Move not Possible");
						return;
					}
				break;
				case 2:
					if(peg_B.isEmpty()){
						cd = peg_C.pop();
						peg_B.push(cd);
					}
					else
					if(peg_B.peek().getWeight()>nd){
						cd = peg_C.pop();
						peg_B.push(cd);
					}
					else{
						System.out.print("Sorry Move not Possible");
						return;
					}
				break;
				case 3:
					if(peg_C.isEmpty()){
						cd = peg_C.pop();
						peg_C.push(cd);
					}
					else
					if(peg_C.peek().getWeight()>nd){
						cd = peg_C.pop();
						peg_C.push(cd);
					}
					else{
						System.out.print("Sorry Move not Possible");
						return;
					}
				break;
			}
				
			}
			else{
				System.out.print("Sorry Cannot Move this Disk");
				return;
			}
		steps++;
	}
	
	static void checkGameOver(){
		Disk disk = null;
		Iterator<Disk> pegC = peg_C.iterator();
		String order = "";
		while (pegC.hasNext()){
			disk = pegC.next();
			order = order + disk.getName();
		}
		
		if(order.equals("D3D2D1")){
			gameOver = true;
		}
	}
	
	static void displayBoard(){

		Iterator<Disk> pegA = peg_A.iterator();
		Iterator<Disk> pegB = peg_B.iterator();
		Iterator<Disk> pegC = peg_C.iterator();
		
		Disk disk = null;
		
		System.out.println("");
		System.out.println("");
		
		System.out.print("Peg A: ");
		while (pegA.hasNext()){
			disk = pegA.next();
			System.out.print(disk.getName()+" ");
		}
		
		System.out.println("");
		System.out.print("Peg B: ");
		while (pegB.hasNext()){
			disk = pegB.next();
			System.out.print(disk.getName()+" ");
		}
		
		System.out.println("");
		System.out.print("Peg C: ");
		while (pegC.hasNext()){
			disk = pegC.next();
			System.out.print(disk.getName()+" ");
		}
	}
	
}

class Disk {

	int weight;
	String name;
	
	public Disk(){
		this.weight = 0;
		this.name = "";
	}
	
	public Disk(int weight, String name) {
		super();
		this.weight = weight;
		this.name = name;
	}

	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
