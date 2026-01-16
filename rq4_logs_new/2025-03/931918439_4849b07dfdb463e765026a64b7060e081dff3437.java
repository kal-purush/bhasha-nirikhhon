package Java_Basic;

public class BinaryToDecimal {
    
    public static void bintodec(int num){
        int mynum = num;
        int deci = 0;
        int power = 0;
        while(mynum != 0){
            int last = mynum%10;
            deci = deci + (last * (int) Math.pow(2,power));
            power++;
            mynum/=10;
        }
        System.out.println(deci);
    }
    
    public static void main(String[] args) {
        bintodec(100);
    }
}