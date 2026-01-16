import java.util.Scanner;

public class Calculate {
    public static void main(String[] args) {
        Scanner inp = new Scanner(System.in);

        System.out.print("Enter the quantity purchased: ");
        int quantity = inp.nextInt();

        System.out.print("Enter the price per item: ");
        double pricePerItem = inp.nextDouble();

        double totalExpenses = calculateTotalExpenses(quantity, pricePerItem);
        System.out.println("Total expenses: $" + totalExpenses);

        inp.close();
    }

    public static double calculateTotalExpenses(int quantity, double pricePerItem) {
        double totalExpenses = quantity * pricePerItem;

        if (quantity > 50) {
            totalExpenses *= 0.90; // 10% discount
        } else if (quantity >= 25 && quantity <= 50) {
            totalExpenses *= 0.95; // 5% discount
        }

        return totalExpenses;
    }
}