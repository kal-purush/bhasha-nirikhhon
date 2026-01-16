package recursao.source;

/**
 * @author Paulo Ricardo Costa da Silva - RA: 1905013 
 * @author Geovane Donizete Laera  - RA: 1902679 
 * @author Marcelo Martinez Mesa Campos - RA: 1905076 
 * @author Isaque Ribeiro dos Santos Junior - RA: 1903978
 * @author Vinícius da Cruz Pera - RA: 1903144
 * Data: 08/09/2021
 *
 */
public class Par {
	// Cria as variáveis element e next
	private int i;
	private int j;
	
	// Método construtor
	public Par(int i, int j) {
		this.i = i;
		this.j = j;
	}
	
	// Métodos Getters
	// Retorna o valor de i
	public int getI() {
		return i;
	}

	// Retorna o valor de j
	public int getJ() {
		return j;
	}
	
	// Métodos Setters
	// Define o valor da variável i
	public void setI(int i) {
		this.i = i;
	}
	
	// Define o valor da variável j
	public void setJ(int j) {
		this.j = j;
	}
	
	// Método que converte a lista para String
	@Override
	public String toString() {
		return "(" + i + ", " + j + ")"; 
	}
}