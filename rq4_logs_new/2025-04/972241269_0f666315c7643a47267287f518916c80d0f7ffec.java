package Filiz;

public class Week02_Finra {
    public static void main(String[] args) {
writeFinra(1,30);

    }
    public static void writeFinra(int start,int end){
        while (start<=end){

            if (start%3==0 && end%5==0){
                System.out.print(" FINRA ");
            } else if (start%3==0) {
                System.out.print(" FIN ");
            } else if (start%5==0) {
                System.out.print(" RA ");
            }else {
                System.out.print(" "+ start +" ");
            }
            start++;
        }
    }

}