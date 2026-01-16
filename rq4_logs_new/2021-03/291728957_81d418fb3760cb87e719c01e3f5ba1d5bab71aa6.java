package jl.forthem.models;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ItemTest {
	private static Item item;
	private static Item item_identical;
	private static Item item_different1;
	private static Item item_different2;
	private static Item item_different3;

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		item =  new Item(1,"An item",1 );
		item_identical =  new Item(1,"An item",1 );
		item_different1 =  new Item(2,"An item",1 );
		item_different2 =  new Item(1,"An other item",1 );
		item_different3 =  new Item(1,"An item",2 );
	}

	
	@Test
	void test_Identical_Items() {
		assertTrue(item.equals(item_identical));
	}

}