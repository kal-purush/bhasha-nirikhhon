public class LRU {
	int size;
	int[][] table;
	int[] priority;
	
	public LRU(int n) {
		this.size = n;
		this.table = new int[size][size];
		this.priority = new int[size];
		
		for(int i = 0; i < size; i++) {
			table[n-1][i] = 2;
		}
		
		for(int i = 0; i < size; i++) {
			table[i][n-1] = 2;
		}
	}
	
	public void reference(int n) {
		if(n > size || n == 0) {
			System.out.println("Your requested page is not in the table.");
		} else {
			for(int i = 0; i < size; i++) {
				table[n-1][i] = 1;
			}
			for(int i = 0; i < size; i++) {
				table[i][n-1] = 0;
			}
			
			for(int i = 0; i < size; i++) {
				priority[i] = 0;
			}
			for(int i = 0; i < size; i++) {
				int a = 0;
				for(int j = 0; j < size; j++) {
					if(table[i][j] == 1) {
						a++;
						priority[i] = a;
					} else {
						priority[i] = a;
					}
				}
			}
		}
		
	}
	
	public int findPage() {
		int page = 0;
		int check = size;
		
		for(int i = 0; i < size; i++) {
			boolean pageCheck = true;
			
			for(int j = 0; j < size; j++) {
				if(table[i][j] == 2) {
					pageCheck = false;
				}
			}
			
			if(pageCheck == true && priority[i] < check) {
				page = i + 1;
				check = priority[i];
			}
		}
		return page;
	}
	
	public void print() {
		
		// Binary array print
		
		for(int i = 0; i < size; i++) {
			for(int j = 0; j < size; j++) {
				if(table[i][j] == 2) {
					System.out.println(0 + " ");
				} else {
					System.out.print(table[i][j] + " ");
				}
			}
			System.out.println();
		}
		
		// Priority array print
		
		for(int i = 0; i < size; i++) {
			System.out.print(priority[i] + " ");
		}
		
		System.out.println();
		
		// LRU print
		
		int page = 0;
		int check = size;
		
		for(int i = 0; i < size; i++) {
			boolean checkPage = true;
			
			for(int j = 0; j < size; j++) {
				if(table[i][j] == 2) {
					checkPage = false;
				}
			}
			
			if(checkPage == true && priority[i] < check) {
				page = i + 1;
				check = priority[i];
			}
		}
		
		System.out.println("Last Recently Used is: " + page);
	}
}