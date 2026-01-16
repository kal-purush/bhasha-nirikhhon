import java.util.*;
import java.io.*;


class ToLowerCase {

	public static String stringToLower(String str) {
		for (int i=0; i<str.length(); ++i) {
            if (str.charAt(i) <= 'Z' && str.charAt(i) >= 'A') {
                int ascii = (int)str.charAt(i) + 32;
                str = str.replace(str.charAt(i), (char)ascii);
            }
        }
        return str;

	}

	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		String s = br.readLine();
		String ans = stringToLower(s);

		System.out.println(ans);

	}
}