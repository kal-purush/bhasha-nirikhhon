import java.util.Scanner;

public class TheCircleCenter {
    public static void main(String[]args){
        int heightOfBlock, widthOfBlock;
        Scanner scan = new Scanner(System.in);
        System.out.println("Enter the height:");
        heightOfBlock = scan.nextInt();
        System.out.println("Enter the width:");
        widthOfBlock = scan.nextInt();
        
    for (int i = -heightOfBlock; i < heightOfBlock; i++) {
        for (int j = -widthOfBlock; j < widthOfBlock; j++) {
            if (j * j + i * i <= heightOfBlock / 2 * widthOfBlock / 2) {
                if (i == 0 && j == 0) {
                        System.out.print(" ");
                    } 
                else {
                        System.out.print("O");
                    }
                }
            else {
                    System.out.print("#");
                }
            }
            System.out.println("");
        }
        scan.close();
    }
}