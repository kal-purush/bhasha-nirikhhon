package ru.aston.tarakanov_aa.task1;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ru.aston.tarakanov_aa.task1.Exception.SetPriceException;

public class CollectionTest {
	
private List<String> arrayList; 
	
	@BeforeEach
	public void initColletctionInstanse() {
		arrayList = new ArrayList<String>();		
		arrayList.add("Olga");
		arrayList.add("Vera");
		arrayList.add("Nastya");				
	}
	
	
	
	@Test
	public void rotateTest() {
		Collections.rotate(arrayList, 1);
		assertEquals(arrayList.get(0), "Nastya");
	}
	
	@Test
	public void fillTest() {
		Collections.fill(arrayList, "Cat");
		assertEquals(arrayList.get(0), "Cat");
	}
	
	@Test
	public void frequencyTest() {
		int freq = Collections.frequency(arrayList, "Vera");
		assertEquals(freq, 1);
	}

}