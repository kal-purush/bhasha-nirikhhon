package reveste.brecho.strategies;

import java.time.LocalDate;

public class CartaoCredito {

    private Double quantidadeValor;
    private String numero;
    private LocalDate dataVencimento;
    private String cvv;

    public CartaoCredito(Double quantidadeValor, String numero, LocalDate dataVencimento, String cvv) {
        this.quantidadeValor = quantidadeValor;
        this.numero = numero;
        this.dataVencimento = dataVencimento;
        this.cvv = cvv;
    }

    public CartaoCredito() {
    }

    public Double getQuantidadeValor() {
        return quantidadeValor;
    }

    public void setQuantidadeValor(Double quantidadeValor) {
        this.quantidadeValor = quantidadeValor;
    }

    public String getNumero() {
        return numero;
    }

    public void setNumero(String numero) {
        this.numero = numero;
    }

    public LocalDate getDataVencimento() {
        return dataVencimento;
    }

    public void setDataVencimento(LocalDate dataVencimento) {
        this.dataVencimento = dataVencimento;
    }

    public String getCvv() {
        return cvv;
    }

    public void setCvv(String cvv) {
        this.cvv = cvv;
    }
}