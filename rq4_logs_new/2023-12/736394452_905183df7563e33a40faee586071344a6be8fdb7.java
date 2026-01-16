package accounts;

import java.math.BigDecimal;

public class SavingsAccount extends Account {
    final static BigDecimal INTEREST_RATE = new BigDecimal(0.6); 

    public SavingsAccount () {

    } 

    public void addInterest() {
        BigDecimal interest = this.getBalance().multiply(INTEREST_RATE); 
        this.setBalance(this.getBalance().add(interest));
        System.out.println("Interest added: $ " + interest); 
    }

    public void withdrawFromSavings (BigDecimal amount) {
        if(amount.compareTo(BigDecimal.ZERO) > 0 && this.getBalance().compareTo(amount) >= 0) {
            this.setBalance(this.getBalance().subtract(amount));
            System.out.println("Sucessfully withdrawn: $ " + amount); 
        } else {
            System.out.println("Invalid amount or insuffient funds.");
        }
    }
    
     
    
}