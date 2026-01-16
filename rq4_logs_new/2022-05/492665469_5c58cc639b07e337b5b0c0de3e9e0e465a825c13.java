public class solution{

    public static void main(String[] args){

        int primeIndex = 10001;
        boolean flag = true;
        long indexCount = 0;
        int indexP = 0;

        while(flag){
            indexCount++;
            if(isPrime(indexCount)){
                indexP++;

                if(indexP==primeIndex){
                    flag = false;

                }

            }

        }
        System.out.println(indexCount);

    }

    public static boolean isPrime(long current){
        if(current == 1){
            return false;
        }

        for(long i=2; i<current; i++){

            if(current%i==0){

                return false;
            }
        }

        return true;
    }

}