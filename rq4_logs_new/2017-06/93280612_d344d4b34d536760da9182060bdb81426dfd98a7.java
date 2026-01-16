
public class Aux {
    public static int[] converteString(String numero1) {
		String conversao[] = numero1.split("");
		int[] binario = new int[conversao.length];
		for (int i = 0; i < conversao.length; i++) {
			binario[i] = Integer.parseInt(conversao[i]);
		}
		return binario;
	}
}