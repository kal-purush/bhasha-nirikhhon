class BankCustomer {
    String name;
    int accountNumber;
 int age;

    public BankCustomer(String name, int accountNumber, int age) {
        this.name = name;
        this.accountNumber = accountNumber;
        this.age = age;
    }

    public void displayDetails() {
        System.out.println("Customer Name: " + name);
        System.out.println("Account Number: " + accountNumber);
        System.out.println("Age: " + age);
    }
}
 public class Main{

    public static void main(String[] args) {
        BankCustomer bs = new BankCustomer("John Doe", 123456, 30);

        bs.displayDetails();
    }
}