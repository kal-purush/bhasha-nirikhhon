package ichim.proiect.cts;

import java.io.FileNotFoundException;

public class Test {

	public static void main(String[] args) {
		//Singleton
		Magazin magazin=Magazin.getInstantaMagazin();
		Magazin magazin1=Magazin.getInstantaMagazin();
		if(magazin==magazin1)
			System.out.println("Acelasi obiect! --> implementare Singleton");
		try {
			//Factory Method 
			Produs carte=magazin.getProdus(TipProdus.CARTE, "Idiotul", 40);
			Produs film=magazin.getProdus(TipProdus.FILM, "Avatar", 100);
			Produs muzica=magazin.getProdus(TipProdus.MUZICA, "Californication", 15);
			
			//Builder
			Client client=new ClientBuilder("Gigel")
					.adaugaEmail("gigel@gmail.com")
					.esteFidel()
					.build();

			//Adapter
			PlataBRD plataCardBRD=new PlataCardBRD();
			PlataCardBCR plataBCR=new AdaptorPlataBanci(plataCardBRD);
			plataBCR.efectueazaPlata();
			System.out.println();
			
			//Decorator
			Produs carteAmbalataCadou=new ProdusAmbalatCadou(carte);
			System.out.println(carteAmbalataCadou.detaliiProdus());
			
			Comanda comanda=new Comanda("comanda initiala");
			comanda.adaugaProdus(carte);
			comanda.adaugaProdus(film);
			comanda.adaugaProdus(muzica);
			comanda.detaliiComanda();
			
			//Strategy
			MetodaPlata metodaPlataBRD=new CardBRD(plataCardBRD);
			metodaPlataBRD.adaugaComanda(comanda);
			client.setMetodaPlata(metodaPlataBRD);
			client.setComanda(comanda);
			
			//Facade
			magazin.finalizareComanda(client, client.getMetodaPlata());
			
			//Proxy
			PlataBRD plataSigura=new PlataCardBRDSigura(plataCardBRD);
			plataSigura.plateste();
			
			magazin.adaugaPlata(client.achita());
			
			//Command
			magazin.inregistreazaPlata();
			
			//Observer
			//State
			comanda.detaliiComanda();
			
		} catch (ExceptieProdusInexistent e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExceptiePINInvalid e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}