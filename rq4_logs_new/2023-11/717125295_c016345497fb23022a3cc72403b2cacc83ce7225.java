package HomeWorks;

public class HomeWork15 {
    public static void main(String[] args) {
        int[] arr1 = {10, 15, 10, 1, 25, 10};
        int[] arr2 = {10, 15, 10, 1, 25, 45};
       System.out.println(checkSumme(arr1)); 
       System.out.println(checkSumme(arr2));
    }

    public static boolean checkSumme(int[] arr1){
        int sum = 0;
        // for(int element : arr1 ){
        //     if(element==10){
        //         sum++;
        //         if(sum==3){
        //             return true;
        //         }
        //     }
        // }
        // return false;
            for(int element : arr1){
                if (element==10) {
                    sum += element;
                    if(sum==30){
                        return true;
                    }
                }
            }return false;

    }
}