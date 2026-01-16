class fibo {
    fibo() {

    }

    public int findFibonacci(int n){
        if(n>0){
            if(n==1){
                return 0;
            }
            if(n==2){
                return 1;
            }
            else{
                return findFibonacci(n-1)+findFibonacci(n-2);
            }
        }
        else{
            System.out.println("Fibonacci does not exist for provided value");
            return 0;
        }
    }
}

public class fibonacci{
    public static void main(String args[]){
        fibo a= new fibo();
        System.out.println(a.findFibonacci(6));
    }
}