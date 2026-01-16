/*
Given a non-negative integer n, count all numbers with unique digits, x, where 0 ≤ x < 10n.


*/

public class CountUniqueDigits {
    public int countNumbersWithUniqueDigits(int n) {

    	int[] table = new int[11];
    	createTable(table);
    	if (n > 10) return table[10];
    	return table[n];

    	//add to the front any (10-n-1) numbers between 0-9: f(n) = (f(n-1)) * (10-n-1) + f(n-2)
    }

    public void createTable(int[] table) {
    	if (table.length < 2) return;
    	table[0] = 1;
    	table[1] = 10;
    	for (int i=2; i<table.length; i++) {
    		table[i] = table[i-1]*(10-(i-1));
    		for (int j=i-2; j>=0; j--) {
    		    table[i]+=table[j];
    		}
    	}
    	return;
    }
}