
package ContaBancaria;

public class ContaCorrente extends Conta {
    
    
    //Metodo
    @Override
    public void atualiza(double p){
    double porcentagem;
        System.out.println("Saldo Atual:"+ this.saldo);
    porcentagem = (p * this.saldo) / 100;
    this.saldo+=porcentagem;
        System.out.println("Saldo Atualizado:" + this.saldo);
    }
    
    
    
    
}
