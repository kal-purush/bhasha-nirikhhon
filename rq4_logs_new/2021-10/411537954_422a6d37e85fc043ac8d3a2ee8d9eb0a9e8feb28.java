package cooper.finance;
import java.util.ArrayList;

public class FinanceManager {
    public static ArrayList<Integer> balanceSheet = new ArrayList<>();


    public FinanceManager() {

    }

    public static void addBalance(int amount) {
        balanceSheet.add(Integer.valueOf(amount));
    }

    public static void printBalanceSheet() {
        for (int i = 0; i < balanceSheet.size(); i++) {
            if (balanceSheet.size() != 0) {
                System.out.println(i + 1 + "." + balanceSheet.get(i));
            }
        }
    }
}