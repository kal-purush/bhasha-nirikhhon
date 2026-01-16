public class Sort {
    public static void bubbleSort(long[] array){
        //进行多少次冒泡过程
        for (int i = 0; i  < array.length-1 ;i++){
            //无序区间 [0，array.length-i）
            //一次冒泡过程，而且只在无序区间进行

            boolean sorted = true;
            //遍历无序区间，做冒泡过程
            for (int j = 0; j < array.length - i -1; j++) {
                if (array[j] > array [j+1]){
                    long tmp = array[j];
                    array[j] = array[j+1];
                    array[j+1] = tmp;
                    //只要进行过交换，无序区间有序的假设就不成立了
                }
            }
            //每次冒泡之后，假设成立，整个数组有序
            if (sorted) {
                break;
            }
        }
    }
    public static void insertSort(long[] array){
        for (int i = 0; i <array.length-1; i++) {
            int j;
            long x = array[i];
            for ( j = i - 1; j >=0 && array[j] >x ; j++) {
                array[j+1] = array[j];
            }
            array[j-1] = x;
        }
    }

}