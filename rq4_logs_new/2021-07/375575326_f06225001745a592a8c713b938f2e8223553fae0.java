public class Client {
	public static void main(String[] args) {
		Suuchi s = new Suuchi();
		Observer o1 = new NishinHyouji();
		Observer o2 = new JyuurokushinHyouji();
		s.attach(o1);
		s.attach(o2);
		int i = 0;
		while (i < 100) {
			s.putValue(i);
			i += (int)(Math.random() * 30) - 10;
		}
	}
}