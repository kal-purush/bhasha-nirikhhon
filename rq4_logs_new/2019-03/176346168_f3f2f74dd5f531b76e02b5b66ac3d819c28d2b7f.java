package tests;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import util.Position;

class PositionTest {
	Position pos1 = new Position(1, 1);
	Position pos2 = new Position(10, 10);
	
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
		pos1 = new Position(0, 0);
		pos2 = new Position(10, 10);
	}

	@Test
	void xGreaterThanTest() {
		pos1.setPosition(pos1.getX(), 0);    // Je set les valeurs de Y à 0 pour être sûr 
		pos2.setPosition(pos2.getY(), 0);	 // que ce soit bien les x qui sont traités
		assertTrue(pos2.xGreaterThan(pos1) > 0);
		assertTrue(pos1.xGreaterThan(pos2) < 0);
		pos2.setX(pos1.getX());
		assertTrue(pos1.xGreaterThan(pos2) == 0);
		assertTrue(pos2.xGreaterThan(pos1) == 0);
	}
	
	@Test
	void yGreaterThanTest() {
		pos1.setPosition(0, pos1.getY());    // Je set les valeurs de X à 0 pour être sûr 
		pos2.setPosition(0, pos2.getY());	 // que ce soit bien les Y qui sont traités
		assertTrue(pos2.yGreaterThan(pos1) > 0);
		assertTrue(pos1.yGreaterThan(pos2) < 0);
		pos2.setY(pos1.getY());
		assertTrue(pos1.yGreaterThan(pos2) == 0);
		assertTrue(pos2.yGreaterThan(pos1) == 0);
	}
	
	@Test
	void setPositionTest() {
		pos1.setPosition(pos2);
		assertTrue(pos1.getX() == pos2.getX());
		assertTrue(pos1.getY() == pos2.getY());
	}

}