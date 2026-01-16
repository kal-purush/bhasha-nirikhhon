package clase;

import interfete.IPlata;

import java.util.ArrayList;

public abstract class ARezervare {
	Zbor zbor=null;
	ArrayList<Client> pasageri=new ArrayList<Client>();
	IPlata modalitatePlata=null;
	
	public void proceduraRezervare(){
		selecteazaZbor();
		inregistrarePasager();
		selecteazaLoc();
		alegeTipPlata();
		confirma();
	}
	
	public abstract void selecteazaZbor();
	public abstract void inregistrarePasager();
	public abstract void selecteazaLoc();
	public abstract void alegeTipPlata();
	public abstract void confirma();
}