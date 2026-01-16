public class Aufg1{
	// Alle Kombinationen von Mischnamen zwischen a und b ausgeben.
	// Bedingung ist, dass aus a min. die ersten zwei Buchstaben und aus
	// b mindestens die letzten zwei Buchstaben als Sub-String verwendet
	// werden. Die Verwendung der kompletten Namen ist ebenfalls untersagt.
	public static void mixen(String a, String b){
		for(int i=0; i<a.length()-2; i++){
			for(int j=0; j<b.length()-2; j++){
				Terminal.println(a.substring(0, 2+i) + b.substring(j+1, b.length()));
			}
		}
	}
	
	public static void main(String[] args){
		String a;
		String b;
		
		try{
			a = args[0];
			b = args[1];
		} catch(ArrayIndexOutOfBoundsException e){
			Terminal.println("Die Anzahl der übergebenen Namen stimmt nicht. Verwende Standardnamen: Brad Angelina");
			a = "Brad";
			b = "Angelina";
		}
		
		Terminal.println("Mix 1:");
		mixen(a,b);
		
		Terminal.println("\nMix 2:");
		mixen(b,a);
	}
}