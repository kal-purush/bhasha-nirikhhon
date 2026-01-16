package Structures;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import util.Surface;

class RoutesTest {
	
	Lieu lieu1 = new Maison(null, null, new Surface(0, 0, 10, 10));
	Lieu lieu2 = new Maison(null, null, new Surface(1, 1, 10, 10));
	Lieu lieu3 = new Maison(null, null, new Surface(2, 2, 10, 10));
	Lieu lieu4 = new Maison(null, null, new Surface(3, 3, 10, 10));
	Lieu lieu5 = new Maison(null, null, new Surface(4, 4, 10, 10));
	
	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
	}

	@BeforeEach
	void setUp() throws Exception {
	}

	@AfterEach
	void tearDown() throws Exception {
		
	}
	
	@Test
	void haveVoisinTest() {
		ArrayList<Lieu> lieus = new ArrayList<>();
		lieus.add(lieu1);
		lieus.add(lieu2);
		lieus.add(lieu3);
		lieus.add(lieu4);
		lieus.add(lieu5);
		Routes routes = new Routes(lieus);
		assertFalse(routes.haveVoisin(lieu1));
		assertFalse(routes.haveVoisin(lieu2));
		assertFalse(routes.haveVoisin(lieu3));
		assertFalse(routes.haveVoisin(lieu4));
		assertFalse(routes.haveVoisin(lieu5));
		routes.addConnexion(lieu1, lieu2);
		assertTrue(routes.haveVoisin(lieu1));
	}

	@Test
	void addConnexionTest() {
		ArrayList<Lieu> lieus = new ArrayList<>();
		lieus.add(lieu1);
		lieus.add(lieu2);
		lieus.add(lieu3);
		lieus.add(lieu4);
		lieus.add(lieu5);
		Routes routes = new Routes(lieus);
		routes.addConnexion(lieu1, lieu2);
		assertTrue(routes.isConnected(lieu1, lieu2));
		assertTrue(routes.isConnected(lieu2, lieu1));
		assertFalse(routes.isConnected(lieu1, lieu5));
		routes.addConnexion(lieu3, lieus);
		assertTrue(routes.isConnected(lieu3, lieu1));
		assertTrue(routes.isConnected(lieu3, lieu2));
		assertTrue(routes.isConnected(lieu3, lieu3));
		assertTrue(routes.isConnected(lieu3, lieu4));
		assertTrue(routes.isConnected(lieu3, lieu5));		
	}
	
	@Test
	void removeVoisinTest() {
		ArrayList<Lieu> lieus = new ArrayList<>();
		lieus.add(lieu1);
		lieus.add(lieu2);
		lieus.add(lieu3);
		lieus.add(lieu4);
		lieus.add(lieu5);
		Routes routes = new Routes(lieus);
		routes.addConnexion(lieu1, lieu2);
		routes.addConnexion(lieu3, lieus);
		assertTrue(routes.haveVoisin(lieu1));
		routes.removeAllVoisin(lieu1);
		assertFalse(routes.haveVoisin(lieu1));
		routes.removeVoisin(lieu3, lieu1);
		assertFalse(routes.isConnected(lieu3, lieu1));
		assertTrue(routes.isConnected(lieu3, lieu2));
		assertTrue(routes.isConnected(lieu3, lieu3));
		assertTrue(routes.isConnected(lieu3, lieu4));
		assertTrue(routes.isConnected(lieu3, lieu5));	
	}
	
	@Test
	void getVoisinsTest() {
		ArrayList<Lieu> lieus = new ArrayList<>();
		lieus.add(lieu1);
		lieus.add(lieu2);
		lieus.add(lieu3);
		lieus.add(lieu4);
		lieus.add(lieu5);
		Routes routes = new Routes(lieus);
		lieus.remove(lieu3);
		routes.addConnexion(lieu3, lieus);
		assertTrue(routes.getVoisins(lieu3).equals(lieus));
	}

}