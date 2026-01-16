public class Q10 {
    public static int getHalf(int num){
        int half;
        if(num%2==0){
            half = num/2;
        }else{
            half=(num-1)/2;
        }
        return half;
    }

    public static boolean checkPrime(int num){
        boolean result = true;
        for(int i=3;i<getHalf(num);i+=2){
            if(num%i==0){
                result = false;
                break;
            }
        }
        return result;
    }

    public static void main(String[] args) {
        final int LIMIT = 2000000;
        long sum=2L;

        for(int i=3;i<LIMIT;i+=2){
            if(checkPrime(i)){
                sum+=i;
            }
        }
        System.out.println(sum);  
    }
}