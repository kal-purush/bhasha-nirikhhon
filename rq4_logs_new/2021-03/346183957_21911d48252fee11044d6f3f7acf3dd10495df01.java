
public class CommandMain {

	public static void main(String[] args) {
		Mittagessen1 me1 = new Mittagessen1();
		Mittagessen2 me2 = new Mittagessen2();
		Mittagessen3 me3 = new Mittagessen3();
		
		Flie�bandarbeiter f = new Flie�bandarbeiter();
		Mechaniker m = new Mechaniker();
		Lagerarbeiter l = new Lagerarbeiter();
		
		
	}

}

public class Flie�bandarbeiter {
	private MittagessenBefehl meBefehl;

	public void setMittagessenBefehl(MittagessenBefehl meBefehl) {
		this.meBefehl = meBefehl;
	}

	public void mittagessenAnzeigen() {
		meBefehl.anzeigen();
	}
}