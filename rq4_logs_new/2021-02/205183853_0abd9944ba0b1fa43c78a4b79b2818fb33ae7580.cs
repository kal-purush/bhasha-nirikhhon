namespace Linear_DataStructures
{
    public class SegregateArray//Easy
    {

        // //Dutch national flag problem.
        // Algorithm: segregateEvenOdd()
        // 1) Initialize two index variables left and right:  
        // left = 0,  right = size -1 
        // 2) Keep incrementing left index until we see an odd number.
        // 3) Keep decrementing right index until we see an even number.
        // 4) If lef < right then swap arr[left] and arr[right]

        public static void SegreateEvenOddArray(int[] a)
        {
            int left = 0;
            int right = a.Length - 1;
            int count = 0;

            while (left < right)
            {
                while (a[left] % 2 == 0 && left < right)//vimportant while loop
                {
                    left++;
                }
                while (a[right] % 2 == 1 && left < right)//vimportant while loop
                {
                    right--;
                }
                if (left < right)
                {
                    int tmp = a[left];
                    a[left] = a[right];
                    a[right] = tmp;
                    left++;
                    right--;
                    count++;
                }
            }

            foreach (int value in a)
            {
                System.Console.Write(value + " ");
            }
            System.Console.WriteLine("count " + count);

        }

        // Hoare’s vs Lomuto partition scheme in QuickSort
        //https://www.geeksforgeeks.org/hoares-vs-lomuto-partition-scheme-quicksort/
        static void RearrangeEvenAndOdd(int[] arr,
                                       int n)
        {
            // variables
            int j = -1, temp;

            // quick sort method
            for (int i = 0; i < n; i++)
            {

                // if array of element
                // is odd then swap
                if (arr[i] % 2 == 0)
                {

                    // increment j by one
                    j++;

                    // swap the element
                    temp = arr[i];
                    arr[i] = arr[j];
                    arr[j] = temp;
                }
            }
        }


        public static void DriverCode()
        {
            int[] a = { 4, 5, 6, 8 };
            SegreateEvenOddArray(a);
            int[] arr = { 12, 34, 45, 9, 8, 90, 3 };
            SegreateEvenOddArray(arr);
            int[] arr2 = { 12, 34, 45, 9, 8, 90, 3 };
            RearrangeEvenAndOdd(arr2,arr2.Length);


        }


    }
}