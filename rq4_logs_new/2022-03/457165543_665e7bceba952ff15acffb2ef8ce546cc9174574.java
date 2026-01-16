class ArrayIgnoreNegativeNumbers {

    static int[]  printArray(int[] array) {

        for(int num : array) {
            System.out.print( num + " ");
        }
        return array;
    }

    public static void main(String arg[]) {
        int array[] = {0,-1,2,5,-3,-6,9,0,1};
        System.out.println(" INPUT :");
        printArray(array);
        int length = array.length;
        int counter = 0;
           for (int i  : array) { 
           // while(counter < length) 
            {
                if (i < 0) {
   //                 System.out.println(" Its a NEGATIVE Number "+ i);
                } else {
     //               System.out.println(" Its a POSITIVE Number "+ i);
                    // array[counter] = array[i]
                    counter++;;
                }
            }
        }

        int pArray[] = new int[counter];
        int j = 0;

        for (int posNumber : array) {
            if (posNumber >= 0) {
                pArray[j] = posNumber;
                j++;
            }
        }
        
        System.out.println(" \n +ve Number OUTPUT :");
        printArray(pArray);
    }
}