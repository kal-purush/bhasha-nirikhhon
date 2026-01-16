package techclallenge5.fiap.com.msGestaoItem.exception;

public class PrecoItemNotFoundException extends RuntimeException {
    public PrecoItemNotFoundException() {
        super("O preco do item não foi encontrado!");
    }
}