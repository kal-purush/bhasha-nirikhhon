
public class Data{
	
	private int pieninPortti = 0;
	private int lukumaara = 0;
	private int[] tiedot;
	
	public Data(int tarvittavaMaara, int portti){
		this.tiedot = new int[tarvittavaMaara];
		this.pieninPortti = portti;
		for(int i = 0; i < tiedot.length; i++){
			this.tiedot[i] = 0;
		}
	}
	/**Lis�� saadut luvut kokonaislukutaulukkoon tiedot niin ett� 
	 * taulukon indeksi osoittaa palvelussa ollutta porttia.
	 * Ottaa kiinni IndexOutOfBounds-poikkeuksen ja tulostaa 
	 * ilmoituksen virheest�.
	 * 
	 * @param luku
	 * @param portti
	 */
	synchronized public void lisaaLuku(int luku, int portti){
		//System.out.println(portti +": lis�t��n luku... " + luku);
		try{
			tiedot[portti-pieninPortti-1]+=luku;
			this.lukumaara++;
		}catch(IndexOutOfBoundsException e){
			System.out.println("VIRHE: Annettua porttia ei l�ydy!");
			e.printStackTrace();
		}
	}
	
/**
 * @return v�litettyjen kokonaislukujen lukum��r�
 */
	public int annaLukumaara(){
		return this.lukumaara;
	}
	
/** Selvitt�� mille summauspalvelimelle v�litettyjen kokonaislukujen
 *  summa on suurin
 *  @return indeksipaikkaa vastaava luku + 1
 */
	public int annaSuurinSummausPalvelin(){
		int suurin = 0;
		for(int i = 1; i < tiedot.length;i++){
			if(tiedot[suurin]<tiedot[i]){
				suurin = i;
			}
		}
		return suurin+1;
	}	
/** 
 * @return v�litettyjen kokonaislukujen summa
 */
	public int annaSumma(){
		int summa = 0;
		for(int i = 0; i < tiedot.length;i++){
			summa += tiedot[i];
		}
		return summa;
	}
	
	public void tulostaTiedot(){
		System.out.println("TULOSTETAAN DATA");
		for(int i = 0; i < tiedot.length; i++){
			System.out.println((pieninPortti+1+i) + ": " + tiedot[i]);
		}
	}
}