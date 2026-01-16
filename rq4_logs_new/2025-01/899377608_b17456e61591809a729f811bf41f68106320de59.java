import java.util.*;

public class BubbleSort {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        String input = scanner.nextLine().trim();
        scanner.close();

        int[] numbers = Arrays.stream(input.split(" ")).mapToInt(Integer::parseInt).toArray();
        List<String> steps = new ArrayList<>();
        int comparisons = 0, swaps = 0;

        BubbleSort sorter = new BubbleSort();
        sorter.sort(numbers, steps, comparisons, swaps);

        Map<String, Object> result = new HashMap<>();
        result.put("steps", steps);
        result.put("comparisons", comparisons);
        result.put("swaps", swaps);
        result.put("complexity", "O(n²)");

        System.out.println(new org.json.JSONObject(result).toString());
    }

    public void sort(int[] array, List<String> steps, int comparisons, int swaps) {
        int n = array.length;
        steps.add(Arrays.toString(array));

        for (int i = 0; i < n - 1; i++) {
            for (int j = 0; j < n - i - 1; j++) {
                comparisons++;
                if (array[j] > array[j + 1]) {
                    swaps++;
                    int temp = array[j];
                    array[j] = array[j + 1];
                    array[j + 1] = temp;
                    steps.add(Arrays.toString(array));
                }
            }
        }
    }
}