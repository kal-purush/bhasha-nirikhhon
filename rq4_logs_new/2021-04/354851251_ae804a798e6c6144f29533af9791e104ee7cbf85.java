package humbaba.cipher;

public class ScytaleCipher implements HumbabaCipher {

	private int columns;
	
	public ScytaleCipher() {
		this.columns = 5;
	}
	
	public ScytaleCipher(int columns) {
		this.columns = columns;
	}
	
	private char[] doScytale(char[] text, boolean wind) {
		char[] result = new char[text.length];
		int j = 0;
		for(int i = 0; i < text.length; i++)
		{
			if(j >= text.length) {
				j -= text.length;
			}
			if(wind) {
				result[i] = text[j]; 
			} else {
				result[j] = text[i]; 
			}
			j = j + columns;
		}
		return result;
	}
	
	@Override
	public char[] encrypt(char[] text) {
		return doScytale(text, false);
	}

	@Override
	public char[] encrypt(String text) {
		char[] charText = text.toCharArray();
		return encrypt(charText);
	}

	@Override
	public char[] decrypt(char[] text) {
		return doScytale(text, true);
	}

	@Override
	public char[] decrypt(String text) {
		char[] charText = text.toCharArray();
		return decrypt(charText);
	}

}