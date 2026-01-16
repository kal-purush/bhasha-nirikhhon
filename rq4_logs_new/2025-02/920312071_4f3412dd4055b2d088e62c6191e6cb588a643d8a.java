import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
    	Scanner sc = new Scanner(System.in);
    	StringBuilder sb = new StringBuilder();
    	String str = sc.nextLine();
    	int length = str.length();
    	int[] alpha = new int[26];
  
    	for(int i = 0; i < length; i++) {
    		alpha[str.charAt(i) - 'A'] += 1;
    	}
    	
    	int flag = 0;
    	char ch = ' ';
    	
    	if(length % 2 == 0) {
    		for(int i = 0; i < 26; i++) {
    			if(alpha[i] % 2 == 0) {
	    			for(int j = 0; j < alpha[i] / 2; j++) {
	    				sb.append((char)(i + 'A'));
	    			}
    			}
    			else {
    				flag = 1;
    				break;
    			}
    		}
    		if(flag == 1) {
    			System.out.println("I'm Sorry Hansoo");
    		}
    		else {
    			System.out.print(sb);
    			System.out.print(sb.reverse());
    		}
    		
    	}
    	
    	else {
    		for(int i = 0; i < 26; i++) {
    			if(alpha[i] % 2 == 0) {
	    			for(int j = 0; j < alpha[i] / 2; j++) {
	    				sb.append((char)(i + 'A'));
	    			}
    			}
    			else {
    				if(flag == 0) {
    					for(int j = 0; j < alpha[i] / 2; j++) {
    	    				sb.append((char)(i + 'A'));
    	    			}
    					ch = (char)(i + 'A');
    					flag += 1;
    				}
    				else {
    					flag += 1;
    					break;
    				}
    			}
    		}
    		if (flag == 2) {
    			System.out.println("I'm Sorry Hansoo");
    		}
    		else {
    			System.out.print(sb);
    			System.out.print(ch);
    			System.out.print(sb.reverse());
    		}
    	}
	}
}