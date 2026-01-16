
public class conta {

    private double saldo;   
    private String numero;
    
    public conta (double saldo, String numero){
        this.saldo = saldo;
        this.numero = numero;
    }

   public void creditar (double valorcredito){
       this.saldo +=  valorcredito;
   }
   
   public void debitar (double valordebito){
       this.saldo -= valordebito; 
   }
   
}