package parquinho;

public class Kid {
    
    String nome;
    int idade;
    
    public Kid(){
    
    }
    
    public String getnome(){
        return nome;
    }
    public int getidade(){
        return idade;
    }
    
    public void setnome (String nome){
     
        this.nome = nome;
    }
    public void setidade (int idade){
     
        this.idade = idade;
    }
    
    
//    void status(){
//        
//        System.out.println("[" + this.getnome() + ":"+this.getidade()+ "]\n");
//    }
    
    
}