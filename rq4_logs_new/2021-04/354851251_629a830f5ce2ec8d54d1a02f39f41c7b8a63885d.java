package humbaba.cipher;

public class AtbashCipher implements HumbabaCipher {

//	private boolean lettersOnly;
	private boolean ignoreCase;
	
	public AtbashCipher() {
//		this.lettersOnly = true;
		this.ignoreCase = true;
	}
	
	public AtbashCipher(boolean ignoreCase) {
//		this.lettersOnly = true;
		this.ignoreCase = ignoreCase;
	}
	
	private char[] performAtbash(char[] text, boolean decrypt) {
		
		byte adjust = 1;
		if(decrypt) {
			adjust = -1;
		}
		for(int i = 0; i < text.length; i++) {
		
			if(text[i] >= 'a' && text[i] <= 'z') {
				
				text[i] += (char) adjust*(2*('a' - text[i]) + 25);
				
				if(this.ignoreCase) {
					text[i] -= 32;
				}
				
			} else if (text[i] >= 'A' && text[i] <= 'Z') {
				
				text[i] += (char) adjust*(2*('A' - text[i]) + 25);
				
			} else if (text[i] != ' ') {
				
				throw new RuntimeException("Input is not letters only");
				
			}
			
		}
		return text;
	}
	
	@Override
	public char[] encrypt(char[] text) {
		return performAtbash(text, false);
	}

	@Override
	public char[] encrypt(String text) {
		char[] charText = text.toCharArray();
		return encrypt(charText);
	}

	@Override
	public char[] decrypt(char[] text) {
		return performAtbash(text, false);
	}

	@Override
	public char[] decrypt(String text) {
		char[] charText = text.toCharArray();
		return decrypt(charText);
	}

}