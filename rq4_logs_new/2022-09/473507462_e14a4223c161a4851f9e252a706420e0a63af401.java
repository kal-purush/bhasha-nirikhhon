package test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Queue;

import model.classes.UniqueQueue;

public class UniqueQueueTest {

	/*
	 * Per la creazione della coda unica abbiamo deciso di sfruttare dei metodi
	 * primitivi che quindi non andremo a testare.
	 * Le modifiche sono state effettuate unicamente nella fase di aggiunta e di rimozione
	 * degli elementi, motivo per il quale i test si concentreranno unicamente su questi metodi
	 */
	
	public void uniqueQueueTests() {
		try {
			//Lanciamo la prima batteria di test
			generalUniqueQueueTest();
			
		} catch (Exception e) {
			//Se arriviamo qui qualcosa è andato storto
			e.printStackTrace();
			fail("Unexpected exception!");
		}
	}

	private void generalUniqueQueueTest() {
		//Iniziamo creando una "codaUnica"
		Queue<Integer> codaUnica = new UniqueQueue<>();
		
		/*
		 * Continiuamo verificando il corretto funzionamento dei metodi di inserimento
		 * testando l'efficacia dal punto di vista del set
		 */
		assertEquals(0, codaUnica.size());
		assertTrue(codaUnica.add(1));
		assertFalse(codaUnica.add(1));
		
		//Testiamo l'efficacia dal punto di vista della coda
		assertTrue(codaUnica.add(2));
		assertEquals(1, codaUnica.peek());
		
		/*
		 * Continiuamo verificando il corretto funzionamento dei metodi di rimozione
		 * testando l'efficacia dal punto di vista della coda, ossia eliminando un elemento
		 * dalla testa, il secondo elemento diventa la nuova testa anche a seguito di
		 * un'aggiunta
		 */
		assertTrue(codaUnica.remove(1));
		assertEquals(2, codaUnica.peek());
		assertTrue(codaUnica.add(1));
		assertEquals(2, codaUnica.peek());
		
		//Creiamo una seconda coda con 10 elementi
		Queue<Integer> secondaCoda = new UniqueQueue<>();
		for(int i = 0; i < 10; i++)
			assertTrue(secondaCoda.add(i));
		
		/*
		 * Verifichiamo il corretto funzionamento dei metodi di inserimento
		 * che restituiranno un booleano true in caso venga aggiunto anche 
		 * solo un elemento
		 */
		assertTrue(codaUnica.addAll(secondaCoda));
		assertFalse(codaUnica.addAll(secondaCoda));
		
		/*
		 * Verifichiamo il corretto funzionamento dei metodi di rimozione
		 * che restituiranno un booleano true in caso venga rimosso anche 
		 * solo un elemento
		 */
		assertTrue(codaUnica.removeAll(secondaCoda));
		assertFalse(codaUnica.removeAll(secondaCoda));
	}
}