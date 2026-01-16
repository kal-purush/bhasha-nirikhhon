package capitulo003;

public class ClasseAccountModificada {
    private double balance;
    public void withdraw (double amountWithdrawn) {
        if (amountWithdrawn > 0 && amountWithdrawn < balance) {
            balance = balance - amountWithdrawn;
        } else {
            System.out.println("Withdrawal amount exceeded account balance");
        }
    }
}