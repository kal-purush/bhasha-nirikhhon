package OrdenacaoAleatoria;

public class OrdenacaoAleatoria {
    
    public void bubbleShuffle(int[] arr) {
        int n = arr.length;
        for (int i = 0; i < n - 1; i++) {
            for (int j = 0; j < n - i - 1; j++) {
                if (Math.random() > 0.5) {  
                    int temp = arr[j];
                    arr[j] = arr[j + 1];
                    arr[j + 1] = temp;
                }
            }
        }
    }

    public void insertionShuffle(int[] arr) {
        for (int i = 1; i < arr.length; i++) {
            int j = (int) (Math.random() * (i + 1));  
            int temp = arr[i];
            arr[i] = arr[j];
            arr[j] = temp;
        }
    }

    public void selectionShuffle(int[] arr) {
        int n = arr.length;
        for (int i = 0; i < n - 1; i++) {
            int j = i + (int) (Math.random() * (n - i));  
            int temp = arr[j];
            arr[j] = arr[i];
            arr[i] = temp;
        }
    }

    public void mergeShuffle(int[] arr, int l, int r) {
        if (l < r) {
            int m = l + (r - l) / 2;
            mergeShuffle(arr, l, m);
            mergeShuffle(arr, m + 1, r);
            merge(arr, l, m, r);
    
            
            if (Math.random() > 0.5) {
                int i = l + (int)(Math.random() * (r - l + 1));
                int j = l + (int)(Math.random() * (r - l + 1));
                int temp = arr[i];
                arr[i] = arr[j];
                arr[j] = temp;
            }
        }
    }
    
    public static void merge(int[] arr, int l, int m, int r) {
        int n1 = m - l + 1;
        int n2 = r - m;

        int[] L = new int[n1];
        int[] R = new int[n2];

        for (int i = 0; i < n1; ++i)
            L[i] = arr[l + i];
        for (int j = 0; j < n2; ++j)
            R[j] = arr[m + 1 + j];

        int i = 0, j = 0;

        int k = l;
        while (i < n1 && j < n2) {
            if (L[i] <= R[j]) {
                arr[k] = L[i];
                i++;
            } else {
                arr[k] = R[j];
                j++;
            }
            k++;
        }

        while (i < n1) {
            arr[k] = L[i];
            i++;
            k++;
        }

        while (j < n2) {
            arr[k] = R[j];
            j++;
            k++;
        }
    }

    public void quickShuffle(int[] arr, int low, int high) {
        if (low < high) {
            int pi = partition(arr, low, high);
            quickShuffle(arr, low, pi - 1);
            quickShuffle(arr, pi + 1, high);
        }
    }
    
    public static int partition(int[] arr, int low, int high) {
        int pivotIndex = low + (int)(Math.random() * (high - low + 1));  
        int pivot = arr[pivotIndex];
        arr[pivotIndex] = arr[high];
        arr[high] = pivot;
    
        int i = (low - 1);
        for (int j = low; j < high; j++) {
            if (Math.random() > 0.5) {  
                i++;
                int temp = arr[i];
                arr[i] = arr[j];
                arr[j] = temp;
            }
        }
    
        int temp = arr[i + 1];
        arr[i + 1] = arr[high];
        arr[high] = temp;
    
        return i + 1;
    }
    
    public void heapShuffle(int[] arr) {
        int n = arr.length;
    
        for (int i = n / 2 - 1; i >= 0; i--)
            heapify(arr, n, i);
    
        for (int i = n - 1; i > 0; i--) {
            int temp = arr[0];
            arr[0] = arr[i];
            arr[i] = temp;
    
            if (Math.random() > 0.5) {  
                heapify(arr, i, 0);
            }
        }
    }
    
    public static void heapify(int[] arr, int n, int i) {
        int largest = i;
        int left = 2 * i + 1;
        int right = 2 * i + 2;
    
        if (Math.random() > 0.5 && left < n) largest = left;
        if (Math.random() > 0.5 && right < n && right != largest) largest = right;
    
        if (largest != i) {
            int swap = arr[i];
            arr[i] = arr[largest];
            arr[largest] = swap;
    
            heapify(arr, n, largest);
        }
    }    
}