package undici.jUntisTests;


import org.junit.Test;

import junit.framework.Assert;
import undici.kunde.KundeJBDBCDao;

public class IstDieEmailAdresseSchonVerwendetTest {

	@SuppressWarnings("deprecation")
	@Test
	public void test() {
		KundeJBDBCDao dbKunde = new KundeJBDBCDao();
		String email = "tobias.fatzer@bbcag.ch";
		
		
		//wenn R�ckgabe false: email Adresse bereits verhanden.
		//wenn R�ckgabe true: email Adresse noch nicht verhanden.
		Assert.assertEquals(true, dbKunde.EmailSchonVorhanden(email));
	}

}