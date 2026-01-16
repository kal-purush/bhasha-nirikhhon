package test;

import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;

import model.Asiakas;
import model.dao.Dao;

@TestMethodOrder(OrderAnnotation.class)
class JUnit_testaa_asiakkaat {

	@Test
	@Order(1) 
	public void testPoistaKaikkiAsiakkaat() {
		//Poistetaan kaikki asiakkaat
		Dao dao = new Dao();
		dao.poistaKaikkiAsiakkaat("nimda");
		ArrayList<Asiakas> asiakkaat = dao.listaaKaikki();
		assertEquals(0, asiakkaat.size());
	}
	
	@Test
	@Order(2) 
	public void testLisaaAsiakas() {		
		//Tehd��n muutama uusi testiasiakas
		Dao dao = new Dao();		
		Asiakas asiakas_1 = new Asiakas(1,"Hermione", "Granger", "123456", "hg@tylypahka.com");
		Asiakas asiakas_2 = new Asiakas(1,"Ronald", "Weasley", "963852", "rw@tylypahka.com");
		Asiakas asiakas_3 = new Asiakas(1,"Draco", "Malfoy", "666666", "dm@tylypahka.com");
		assertEquals(true, dao.lisaaAsiakas(asiakas_1));
		assertEquals(true, dao.lisaaAsiakas(asiakas_2));
		assertEquals(true, dao.lisaaAsiakas(asiakas_3));
	}
	
	@Test
	@Order(3) 
	public void testMuutaAsiakas() {
		//Muutetaan yht� asiakasta
		Dao dao = new Dao();
		Asiakas muutettava = dao.etsiAsiakas(15);
		muutettava.setEtunimi("Harry");
		muutettava.setSukunimi("Potter");
		muutettava.setPuhelin("111111");
		muutettava.setSposti("hp@posti.com");
		dao.muutaAsiakas(muutettava, 15);	
		assertEquals("Harry", dao.etsiAsiakas(15).getEtunimi());
		assertEquals("Potter", dao.etsiAsiakas(15).getSukunimi());
		assertEquals("111111", dao.etsiAsiakas(15).getPuhelin());
		assertEquals("hp@posti.com", dao.etsiAsiakas(15).getSposti());
	}
	
	@Test
	@Order(4) 
	public void testPoistaAsiakas() {
		Dao dao = new Dao();
		dao.poistaAsiakas(15);
		assertEquals(null, dao.etsiAsiakas(15));
	}

}