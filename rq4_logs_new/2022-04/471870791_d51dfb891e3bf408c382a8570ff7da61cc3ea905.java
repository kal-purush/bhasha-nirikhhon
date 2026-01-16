import auxava.util.Scanner;

class Main {
  public static void main(String[] args) {
    int N, X, Y, soma;
    
    Scanner reader = new Scanner(System.in);
    N = reader.nextInt();
    
    for (int i = 0; i < N; i++) {
    	soma = 0;
    	X = reader.nextInt();
    	Y = reader.nextInt();
    	
    	if (Y > X) {
	  		for (int aux = X+1; aux < Y; aux++) {
	  			if (aux % 2 != 0) soma += aux;
	  		}
    	} else {
	  		for (int aux = Y+1; aux < X; aux++) {
	  			if (aux % 2 != 0) soma += aux;
	  		}
    	}
    	
    	System.out.println(soma);
    }
 
  }
}