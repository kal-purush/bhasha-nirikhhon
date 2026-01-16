package model;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class MineTest {
	
	/** The mine that contain the list of elements to test*/
	private Mine mine;
	
	/**
	 * Instantiate a new mMine before each test
	 * @throws Exception
	 * 		Exception if the build of the mine failed
	 */
	@Before
	public void setUp() throws Exception {
		this.mine = new Mine(new BoulderDashModel());
	}

	/**
	 * Check if it's possible to set an element in the elements list
	 * @throws Exception
	 * 		Exception in case of out of range position
	 */
	@Test
	public void testSetElement() throws Exception {
		IElement element = new Stone(new Position(1,1,10,10), this.mine);
		this.mine.setElement(1, 1, element);
		assertEquals(element,this.mine.getElements()[1][1]);
	}

	/**
	 * Check if it's possible to destroy an element in the elements list
	 * @throws Exception
	 * 		Exception in case of out of range position
	 */
	@Test
	public void testDestroyElement() throws Exception {
		this.mine.destroyElement(this.mine.getElements()[5][5]);
		assertEquals(model.Background.class, this.mine.getElements()[5][5].getClass());
	}

	/**
	 * Check if it's possible to add an element in the elements list
	 * @throws Exception
	 * 		Exception in case of out of range position
	 */
	@Test
	public void testAddEnemy() throws Exception {
		IElement element = new Enemy(new Position(1,1,10,10), this.mine);
		this.mine.addEnemy(element);
		assertEquals(element,this.mine.getEnemy().get(this.mine.getEnemy().size()-1));
	}

	/**
	 * Check if it's possible to add an element in the gravity elements list
	 * @throws Exception
	 * 		Exception in case of out of range position
	 */
	@Test
	public void testAddGravity() throws Exception {
		IElement element = new Stone(new Position(1,1,10,10), this.mine);
		this.mine.addGravity(element);
		assertEquals(element,this.mine.getGravity().get(this.mine.getGravity().size()-1));
	}

	/**
	 * Check if it's possible to get the hero from the elements list
	 */
	@Test
	public void testGetHero() {
		Hero hero = Hero.getInstance();
		assertEquals(hero,this.mine.getHero());
	}

}