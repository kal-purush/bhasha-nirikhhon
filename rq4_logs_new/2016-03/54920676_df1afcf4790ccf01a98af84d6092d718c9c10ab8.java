/**
 * @author Nihanshu Purohit
 * @date October, 2015
 * Boolean Satisfiablity Solver.
 */

import java.util.*;

public class Main {


	public static boolean[] array;
	public static void main(String[] args) {
		Scanner s = new Scanner(System.in);
		while(s.hasNextLine()){
			String probname = s.nextLine();
			int numVariables = s.nextInt();
			int numClauses = s.nextInt();
			ArrayList<String> tempclause = new ArrayList<String>(); 
			s.nextLine();
			for (int k =0; k < numClauses; k++){
				tempclause.add(s.nextLine());
			}

			String[] tok = null;
			ArrayList<Integer> parsed = new ArrayList<Integer>();
			ArrayList<ArrayList<Integer>> individualclause = new ArrayList<ArrayList<Integer>>();
			int a =0;
			int sze = tempclause.size();
			while(a< sze){
				String string = tempclause.get(a);
				tok = string.split(" ");

				int c = 0;
				while( c <tok.length){
					parsed.add(Integer.parseInt(tok[c]));
					c++;

				}
				//System.out.println(parsed);
				individualclause.add((ArrayList<Integer>)parsed.clone());
				parsed.clear();
				a++;
			}
			System.out.println();
			array = new boolean[numVariables];

			int j =0;
			while(j < array.length ) {

				array[j] = true;      
				j++;
			}


			System.out.println(probname + ": " + numVariables + " Variable(s) " + numClauses + " Clause(s)" );
			boolean answer = solver(individualclause,array,0);
			if (answer== true){
				System.out.println("Satisfiable");
				int x =0;
				while(x < array.length){
					System.out.print(array[x] + " ");
					x++;
				}
			}
			else 
				System.out.print("Unsatisfiable");
		}


	}
	public static boolean solver(ArrayList<ArrayList<Integer>> individualclause,boolean[] help, int length ){
		int x = help.length;
		if(length >= x){
			return checker(individualclause, help);

		}
		else{
			help[length]= false;
			if(solver(individualclause, help, length+1)){
				return true;
			}
			else {
				help[length] = true;
				return solver(individualclause,help,length+1);
			}
		}

	}

	public static boolean checker(ArrayList<ArrayList<Integer>> individualclause,boolean[] help){
		check: for(int index=0; index<individualclause.size();index++){
			int i =0;		
			while( i < individualclause.get(index).size()){
				int a = individualclause.get(index).get(i);
				int b = (int) ((a <= 0.0D)?0.0D - a : a);
				if(a > 0 == help[b-1]){
					continue check;
				}
				i++;
			}	
			return false;
		}
	return true;
	}
}