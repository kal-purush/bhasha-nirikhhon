package kadai_024;

public class JyankenExec_Chapter24 {
	public static void main(String[] args) {
		Jyanken_Chapter24 jyanken = new Jyanken_Chapter24();
		String myChoice = jyanken.getMyChoice();
		String random = jyanken.getRandom();
		jyanken.playGame(myChoice, random);
		}
}