package Aula05;


public class ContaBanco {
    //Atributos
    public int numConta;
    protected  String tipo;
    private String dono;
    private float saldo;
    private boolean status;

    // Métodos Personalizados
    public void estadoAtual(){
        System.out.println("Conta: " + this.getNumConta());
        System.out.println("Tipo:" + this.tipo );
        System.out.println("Dono:" + this.getDono());
        System.out.println("Saldo:" + this.getSaldo());
        System.out.println("Status :" + this.getStatus());

    }
    public void abrirConta(String tipo){
        this.setTipo(tipo);
        this.setStatus(true);
        if(tipo == "CC") {
            this.setSaldo(50);
        } else if (tipo.equals("CP"))
            this.setSaldo(150);
        System.out.println("Conta aberta com sucesso!");
    }
    public void fecharConta(){
        if (this.getSaldo() > 0){
            System.out.println("Conta não pode ser fechada porque ainda tem dinheiro");
        } else if (this.getSaldo() < 0 ) {
            System.out.println("Conta não pode ser fechada pois tem débtos");
        } else {
            this.setStatus(false);
            System.out.println("Conta fechada com sucesso!");
        }
    }
    public void depositar(float v){
        if (this.getStatus()){
            this.setSaldo(this.getSaldo()+ v);
            System.out.println("Deposito realizado com sucesso");
        }else {
            System.out.println("Impossivel depositar em uma conta fechada");
        }

    }
    public void sacar(float v){
        if (this.getStatus()){
            if (this.getSaldo() >= v){
                this.setSaldo(this.getSaldo() - v);
                System.out.println("Saldo realizado na conta de " + this.getDono());
            }else {
                System.out.println("Saldo insulficiente para saque ");
            }
        }else {
            System.out.println("Impossivel sacar de uma conta fechada ");
        }

    }
    public void pagarMensal(){
        int v = 0;
        if(this.getTipo() == "CC") {
            v = 12;

        } else if (this.getTipo() == "CP") {
            v = 20;
        }
        if (this.getStatus()) {
            this.setSaldo(this.getSaldo() - v);
            System.out.println("Mensalidade paga com sucesso" + this.getDono());
        }else {
            System.out.println("Imposssivel pagar com uma conta fechada");
        }
        {

        }

    }
    // Métodos Especiais


    public ContaBanco() {
        this.setSaldo(0);
        this.setStatus(false);
    }

    public String getDono() {
        return dono;
    }

    public void setDono(String dono) {
        this.dono = dono;
    }

    public int getNumConta() {
        return numConta;
    }

    public void setNumConta(int numConta) {
        this.numConta = numConta;
    }

    public float getSaldo() {
        return saldo;
    }

    public void setSaldo(float saldo) {
        this.saldo = saldo;
    }

    public boolean getStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    public String getTipo() {
        return tipo;
    }

    public void setTipo(String tipo) {
        this.tipo = tipo;
    }
}




