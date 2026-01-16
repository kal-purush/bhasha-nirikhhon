public class HourGlass{

public static void main(String[] args) {
	int numberOfColonsInTheFirstLine = 8;
//print line 1
lineWithQuotes(numberOfColonsInTheFirstLine);
//print the top half
printTopHalf(numberOfColonsInTheFirstLine);
System.out.println("     ||");
printBottomHalf(numberOfColonsInTheFirstLine);
lineWithQuotes(numberOfColonsInTheFirstLine);
//print line 6 (middle)
//print bottom half
//print last line (looks just like line 1)
}


public static void lineWithQuotes(int numberOfColonsInTheFirstLine){
System.out.print("|");
for(int j = 0; j<numberOfColonsInTheFirstLine+2; j++){
System.out.print("\"");
}
System.out.println("|");	
}	

public static void printTopHalf(int numberOfColonsInFirstLine){
for (int i=1; i<=numberOfColonsInFirstLine/2;i++){ //print the first 4 lines
//Print the spaces
for (int s=1; s<=i;s++){
System.out.print(" ");
}
//Print the \
System.out.print("\\");
// Print the colons
for (int k=1; k<=(numberOfColonsInFirstLine+2)-2*i;k++){
System.out.print(":");
}
//Print the /
System.out.println("/");
}
}
public static void printBottomHalf(int numberOfColonsInFirstLine){
	for (int i=numberOfColonsInFirstLine/2; i>=1;i--){ //print the first 4 lines
		//Print the spaces
		for (int s=1; s<=i;s++){
		System.out.print(" ");
		}
		//Print the \
		System.out.print("/");
		// Print the colons
		for (int k=1; k<=(numberOfColonsInFirstLine+2)-2*i;k++){
		System.out.print(":");
		}
		//Print the /
		System.out.println("\\");
		}
}
}