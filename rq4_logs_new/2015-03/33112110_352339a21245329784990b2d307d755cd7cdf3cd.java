package tp7;

public class CompteCourant extends Compte {
	
	private double decouvert;
	
	public CompteCourant (){

	}
	
	public CompteCourant (Client unClient, int unNumero, double unSolde, double unDecouvert){
		super(unClient,unNumero,unSolde);
		this.decouvert = unDecouvert;
	}
	
	public void crediterSolde {
		
	}
}