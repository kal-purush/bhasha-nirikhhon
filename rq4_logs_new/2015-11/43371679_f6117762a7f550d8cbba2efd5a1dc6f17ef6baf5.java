import static org.junit.Assert.*;

import org.junit.Test;


public class AdvantageTest {

	@Test
	public void advantageConstructorTest() {
		String name ="Snabbhet";
		String description = "Du �r snabb";
		int pointCost = 15;
		Advantage adv = new Advantage(name, description, pointCost);
		assertEquals(adv.getName().equals(name), true);
		assertEquals(adv.getDescription().equals(description), true);
		assertEquals(adv.getPointCost() == pointCost, true);
	}

	@Test
	public void getNameTest() {
		String name ="Snabbhet";
		String description = "Du �r snabb";
		int pointCost = 15;
		Advantage adv = new Advantage(name, description, pointCost);
		assertEquals(adv.getName().equals(name), true);
	}

	@Test
	public void setNameTest() {
		String name ="Snabbhet";
		String description = "Du �r snabb";
		int pointCost = 15;
		Advantage adv = new Advantage(name, description, pointCost);
		String newName ="Albert";
		adv.setName(newName);
		assertEquals(adv.getName().equals(newName), true);
	}

	@Test
	public void getDescriptionTest() {
		String name ="Snabbhet";
		String description = "Du �r snabb";
		int pointCost = 15;
		Advantage adv = new Advantage(name, description, pointCost);
		assertEquals(adv.getDescription().equals(description), true);
	}

	@Test
	public void setDescriptionTest() {
		String name ="Snabbhet";
		String description = "Du �r snabb";
		int pointCost = 15;
		Advantage adv = new Advantage(name, description, pointCost);
		String newDescription ="Albert";
		adv.setDescription(newDescription);
		assertEquals(adv.getDescription().equals(newDescription), true);
	}

	@Test
	public void getPointCostTest() {
		String name ="Snabbhet";
		String description = "Du �r snabb";
		int pointCost = 15;
		Advantage adv = new Advantage(name, description, pointCost);
		assertEquals(adv.getPointCost()==(pointCost), true);
	}

	@Test
	public void setPointCostTest() {
		String name ="Snabbhet";
		String description = "Du �r snabb";
		int pointCost = 15;
		Advantage adv = new Advantage(name, description, pointCost);
		int newPointCost =10;
		adv.setPointCost(newPointCost);
		assertEquals(adv.getPointCost()==newPointCost, true);
	}

	@Test
	public void toStringTest() {
		String name ="Snabbhet";
		String description = "Du �r snabb";
		int pointCost = 15;
		Advantage adv1 = new Advantage(name, description, pointCost);
		Advantage adv2 = new Advantage(name, description, pointCost);
		assertEquals(adv1.toString().equals(adv2.toString()), true);
	}

}