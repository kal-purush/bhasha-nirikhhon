import java.util.Scanner;
class Nomor1 {
    public static void main(String[] args) {
    Scanner obj = new Scanner(System.in);
    System.out.println("Masukkan nilai pertama :");
    int x = obj.nextInt();
    System.out.println("Masukkan nilai kedua :");
    int y = obj.nextInt();
    System.out.printf("FPB dari %d dan %d = %d", x , y , cariFPB(x,y));
    }
    public static int cariFPB(int x,int y){
		int z = 0;
		for(int i = 1; i <= ( x < y ? y:x ); i++){
			if(x % i == 0 && y % i == 0)
				z = i;
    }
    return z;
}
}