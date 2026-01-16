package reilaender.test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;

import sprueche.SpruchSammlung;

public class SpruchSammlungTest {

	private SpruchSammlung w;
	
	@Before
	public void setUp() throws Exception {
		w = mock(sprueche.SpruchSammlung.class);
	}

	@Test
	public void testReadFirstSaying() {
		String expected = "asd";
		when(w.readFirstSaying()).thenReturn(expected);
		String actual = w.readFirstSaying();
		assertEquals(expected, actual);
	}

	@Test
	public void testReadLastSaying() {
		String expected = "asd";
		when(w.readLastSaying()).thenReturn(expected);
		String actual = w.readLastSaying();
		assertEquals(expected, actual);
	}

	@Test
	public void testReadRandomSaying() {
		String expected = "asd";
		when(w.readRandomSaying()).thenReturn(expected);
		String actual = w.readRandomSaying();
		assertEquals(expected, actual);
	}

	@Test
	public void testReadFirstLine() {
		String expected = "asd";
		when(w.readFirstLine()).thenReturn(expected);
		String actual = w.readFirstLine();
		assertEquals(expected, actual);
	}
	
	@Test
	public void testReadLastLine() {
		String expected = "asd";
		when(w.readLastLine()).thenReturn(expected);
		String actual = w.readLastLine();
		assertEquals(expected, actual);
	}

	@Test
	public void testReadRandomLine() {
		String expected = "asd";
		when(w.readRandomLine()).thenReturn(expected);
		String actual = w.readRandomLine();
		assertEquals(expected, actual);
	}

	@Test
	public void testClose() {
		w.close();
		verify(w).close();
	}

	@Test
	public void testReadSayingAtLine() {
		String expected = "asd";
		int param = 1;
		when(w.readSayingAtLine(param)).thenReturn(expected);
		String actual = w.readSayingAtLine(param);
		verify(w).readSayingAtLine(param);
		assertEquals(expected, actual);
	}

	@Test
	public void testAppendNewSaying() {
		String param = "7";
		w.appendNewSaying(param);
		verify(w).appendNewSaying(param);
	}

	@Test
	public void testDeleteLastSaying() {
		w.deleteLastSaying();
		verify(w).deleteLastSaying();
	}

	@Test
	public void testDeleteSayingAtLine() {
		int param = 7;
		w.deleteSayingAtLine(param);
		verify(w).deleteSayingAtLine(param);
	}

	@Test
	public void testInsertSayingAtLine() {
		String param = "test";
		int param2 = 42;
		w.insertSayingAtLine(param, param2);
		verify(w).insertSayingAtLine(param, param2);
	}
}