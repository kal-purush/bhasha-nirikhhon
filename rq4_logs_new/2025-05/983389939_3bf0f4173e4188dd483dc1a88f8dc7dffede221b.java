class TransactionHistory {
    int transactionId;
    String senderName;
    String receiverName;
    double amount;
    String status;
   String date;

   public TransactionHistory(int transactionId, String senderName, String receiverName, 
                             double amount, String status, String date) {
       this.transactionId = transactionId;
       this.senderName = senderName;
       this.receiverName = receiverName;
       this.amount = amount;
       this.status = status;
       this.date = date;
   }

   public void displayTransactionDetails() {
       System.out.println("Transaction ID: " + transactionId);
       System.out.println("Sender Name: " + senderName);
       System.out.println("Receiver Name: " + receiverName);
       System.out.println("Amount: $" + amount);
       System.out.println("Status: " + status);
       System.out.println("Date: " + date);
   }
}
public class Main2{
   public static void main(String[] args) {

       
       TransactionHistory t= new TransactionHistory(1001, "Alice", "Bob", 250.75, "Success", "09-08'2025");

       
       t.displayTransactionDetails();
   }
}