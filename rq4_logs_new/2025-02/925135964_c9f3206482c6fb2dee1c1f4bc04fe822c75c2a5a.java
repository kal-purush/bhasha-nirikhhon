import java.util.ArrayList;

public class Main {
    public static void main(String[] args) {
        // Array of stock prices for 10 days
        int[] stockPrice = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
        ArrayList<Integer> stockPrices = new ArrayList<>();

        // Converting array elements to ArrayList
        for (int price : stockPrice) {
            stockPrices.add(price);
        }

        // Performing calculations and displaying results
        System.out.println("Average Price: " + calculateAveragePrice(stockPrice));
        System.out.println("Maximum Price: " + findMaximumPrice(stockPrice));
        System.out.println("Occurrences of 50: " + countOccurrences(stockPrice, 50));
        System.out.println("Cumulative Sum: " + computeCumulativeSum(stockPrices));
    }

    // Method to calculate the average stock price
    public static double calculateAveragePrice(int[] stockPrice) {
        int total = 0;
        for (int price : stockPrice) {
            total += price;
        }
        return (double) total / stockPrice.length;
    }

    // Method to find the maximum stock price
    public static int findMaximumPrice(int[] stockPrice) {
        int max = stockPrice[0];
        for (int price : stockPrice) {
            if (price > max) {
                max = price;
            }
        }
        return max;
    }

    // Method to count occurrences of a specific price
    public static int countOccurrences(int[] stockPrice, int targetPrice) {
        int count = 0;
        for (int price : stockPrice) {
            if (price == targetPrice) {
                count++;
            }
        }
        return count;
    }

    // Method to compute the cumulative sum
    public static ArrayList<Integer> computeCumulativeSum(ArrayList<Integer> stockPrices) {
        ArrayList<Integer> cumulativeSum = new ArrayList<>();
        int sum = 0;
        for (int price : stockPrices) {
            sum += price;
            cumulativeSum.add(sum);
        }
        return cumulativeSum;
    }
}