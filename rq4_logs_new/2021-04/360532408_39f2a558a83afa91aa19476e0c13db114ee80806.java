package acoplamento;

// Interface criada para melhorar o acoplamento do c�digo. Ao inv�s de se acoplar ao EnviadorDeEmail e NotaFiscalDao,
// a classe GeradorDeNotaFiscal ir� se acoplar a esta interface que ser� mais est�vel.

public interface AcaoAposGerarNota {
	
	void executa(NotaFiscal nf);

}