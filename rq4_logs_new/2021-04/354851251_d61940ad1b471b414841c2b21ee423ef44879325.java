package humbaba.cipher;

public class CaesarCipher implements HumbabaCipher {

	private short shift;
	private boolean lettersOnly;
	private boolean ignoreCase;
	
	public CaesarCipher() {
		this.shift = 3;
		this.lettersOnly = true;
		this.ignoreCase = true;
	}
	
	private static char performShift(char c, int shift, char lowerLim, char upperLim) {
		
		c += shift;
		if(c > upperLim) {
			c -= 26;
		} else if (c < lowerLim) {
			c += 26;
		}
		
		return c;
		
	}
	
	private char[] performCaesar(char[] text, short adjustedShift) {
		
		for(int i = 0; i < text.length; i++) {
			
			if(this.lettersOnly) {
				
				if(text[i] >= 'a' && text[i] <= 'z') {
					
					text[i] = performShift(text[i], adjustedShift, 'a', 'z');
					
					if(this.ignoreCase) {
						text[i] -= 32;
					}
					
				} else if (text[i] >= 'A' && text[i] <= 'Z') {
					
					text[i] = performShift(text[i], adjustedShift, 'A', 'Z');
					
				} else if (text[i] != ' ') {
					
					throw new RuntimeException("Input is not letters only");
					
				}
				
			} else {
				throw new RuntimeException("All ASCII not yet implemented");
			}
			
		}

		return text;
	}
	
	@Override
	public char[] encrypt(char[] text) {
		short adjustedShift = this.shift;
		return performCaesar(text, adjustedShift);
	}

	@Override
	public char[] encrypt(String text) {
		char[] charText = text.toCharArray();
		return encrypt(charText);
	}

	@Override
	public char[] decrypt(char[] text) {
		short adjustedShift = (short) (this.shift * -1);
		return performCaesar(text, adjustedShift);
	}

	@Override
	public char[] decrypt(String text) {
		char[] charText = text.toCharArray();
		return decrypt(charText);
	}

}