public final class Joueur {
	private final int id;
	private final char symbole;

	private static int incr = 1;

	public Joueur(char p_symbole){
		this.id = incr++;
		this.symbole = p_symbole;
	}

	public int getid(){
		return this.id;
	}

	public char getsymbole(){
		return this.symbole;
	}
	
	public static void main(String [] Args){
		
	}
}