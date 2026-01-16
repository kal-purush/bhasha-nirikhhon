package books.java_programming.chapter_07.ExampleAccount;

public class CheckingAccount {
    private final String accountNumber;
    private int balance;

    public CheckingAccount(int initialBalance,String accountNumber){
        this.balance = initialBalance;
        this.accountNumber = accountNumber;
    }

    public void deposit(int amount){
        this.balance += amount;

        System.out.println("> Depositing "+amount + " into account : "+accountNumber);
        System.out.println("> New balance : "+balance);

        System.out.println("");
    }

    public void withdraw(int amount){
        System.out.println("> Widthdrawing " + amount+" into account "+accountNumber);

        if(amount > balance){
            System.out.println("> Insufficient funds : ");
        }else{
            this.balance -= amount;
            System.out.println("> New balance : "+balance);
        }
        System.out.println("");
    }

    public int getBalance(){
        return balance;
    }
}