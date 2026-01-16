package Pessoas;


public final class Administrativo extends Funcionario{



    public Administrativo(String nome, String genero, int idade, float salarioBase){

        super(nome, genero, idade, salarioBase);


    }

    @Override

    public double calculaSalario() {

        return super.salarioBase * 1.02;
    }
    
}