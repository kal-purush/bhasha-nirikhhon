package br.com.questao1;

public class Camarote extends Ingresso{

    private double valorAdicional;
    private String localizacao;

    public String getLocalizacao() {
        return localizacao;
    }

    public double  getValorAdicional() {
        return valorAdicional;
    }

    public Camarote(double valorIngresso, double valorAdicional, String localizacao) {
        super(valorIngresso);
        this.valorAdicional = valorAdicional;
    }

    public void setValorAdicional(double valorAdicional) {
        this.valorAdicional = valorAdicional;
    }

    public void setLocalizacao(String localizacao) {
        this.localizacao = localizacao;
    }

    public void imprimeIngresso() {
        System.out.printf("Ingresso Vip - valor: " + (getValorIngresso() + getValorAdicional())
                + " localização " + getLocalizacao());

    }
}

