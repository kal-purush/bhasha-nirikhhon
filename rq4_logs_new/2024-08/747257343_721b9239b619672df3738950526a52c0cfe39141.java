package POTD;

public class FractionAdditionAndSubtraction {
	public static void main(String[] args) {

	}

	public int gcd(int a, int b) {
		if(b == 0) {
			return a;
		}

		return gcd(b, a % b);
	}

	public String fractionAddition(String ex) {
		char[] ar = ex.toCharArray();

		int n = ar.length;

		int den = 1;
		for(int i=0;i<n;) {

			if(ar[i] == '/') {

				int j = i+1;

				while(j < n && ar[j] != '+' && ar[j] != '-') {
					j++;
				}

				den *= Integer.parseInt(ex.substring(i+1, j));
				i = j+1;

			}
			else {
				++i;
			}
		}

		int lst = 0, num = 0;

		for(int i=0;i<n;) {

			if(ar[i] == '/') {

				int j = i-1;
				Integer prev;

				while(j >= 0 && ar[j] != '+' && ar[j] != '-') {
					j--;
				}

				if(j < 0) {
					prev = Integer.parseInt(ex.substring(j+1, i));
				}
				else {
					prev = Integer.parseInt(ex.substring(j+1, i));

					if(ar[j] == '-') {
						prev *= -1;
					}
				}

				j = i+1;

				while(j < n && ar[j] != '+' && ar[j] != '-') {
					j++;
				}

				Integer next = Integer.parseInt(ex.substring(i+1, j));
				num += ((den / next) * prev);
				i = j + 1;
			}
			else {
				++i;
			}
		}

		if(num < 0) {
			int hcf = gcd(Math.min(-num, den), Math.max(-num, den));
			return num / hcf + "/" + den / hcf;
		}

		int hcf = gcd(Math.min(num, den), Math.max(num, den));
		
		return num / hcf + "/" + den / hcf;
	}
}