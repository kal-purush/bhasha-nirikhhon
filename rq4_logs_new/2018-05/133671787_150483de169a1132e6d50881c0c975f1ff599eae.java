package fr.kata.losange.griffoul.mathieu.builder;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import fr.kata.losange.griffoul.mathieu.bo.Diamond;

/**
 * Created by mathieu_griffoul on 17/05/2018.
 */
public class DiamondBuilderTest {

	private DiamondBuilder diamondBuilder;

	@Before
	public void init() {
		this.diamondBuilder = new DiamondBuilder();
	}

	@Test
	public void leftDiamondBuilder_should_return_correct_list_of_string(){

		String s = "abcba";
		Diamond diamond = diamondBuilder.leftDiamondBuilder(s);
		Assert.assertEquals(diamond.getLines().size(), 5);
		Assert.assertEquals(diamond.getLines().get(0), "  a");
		Assert.assertEquals(diamond.getLines().get(1), " b");
		Assert.assertEquals(diamond.getLines().get(2), "c");
		Assert.assertEquals(diamond.getLines().get(3), " b");
		Assert.assertEquals(diamond.getLines().get(4), "  a");

		s = "abcdedcba";
		diamond = diamondBuilder.leftDiamondBuilder(s);

		Assert.assertEquals(diamond.getLines().size(), 9);
		Assert.assertEquals(diamond.getLines().get(0), "    a");
		Assert.assertEquals(diamond.getLines().get(1), "   b");
		Assert.assertEquals(diamond.getLines().get(2), "  c");
		Assert.assertEquals(diamond.getLines().get(3), " d");
		Assert.assertEquals(diamond.getLines().get(4), "e");
		Assert.assertEquals(diamond.getLines().get(5), " d");
		Assert.assertEquals(diamond.getLines().get(6), "  c");
		Assert.assertEquals(diamond.getLines().get(7), "   b");
		Assert.assertEquals(diamond.getLines().get(8), "    a");

	}

	@Test
	public void rightDiamondBuilder_should_return_complete_diamond(){

		List<String> lines = new ArrayList<>();
		lines.add("  a");
		lines.add(" b");
		lines.add("c");
		lines.add(" b");
		lines.add("  a");
		Diamond diamond = new Diamond(lines);

		diamondBuilder.rightDiamondBuilder(diamond);

		Assert.assertEquals(diamond.getLines().size(), 5);
		Assert.assertEquals(diamond.getLines().get(0), "  a");
		Assert.assertEquals(diamond.getLines().get(1), " b b");
		Assert.assertEquals(diamond.getLines().get(2), "c   c");
		Assert.assertEquals(diamond.getLines().get(3), " b b");
		Assert.assertEquals(diamond.getLines().get(4), "  a");
	}

	@Test
	public void rightDiamondBuilder_should_return_complete_diamond_second_case(){

		List<String> lines = new ArrayList<>();
		lines.add("    a");
		lines.add("   b");
		lines.add("  c");
		lines.add(" d");
		lines.add("e");
		lines.add(" d");
		lines.add("  c");
		lines.add("   b");
		lines.add("    a");
		Diamond diamond = new Diamond(lines);

		diamondBuilder.rightDiamondBuilder(diamond);

		Assert.assertEquals(diamond.getLines().size(), 9);
		Assert.assertEquals(diamond.getLines().get(0), "    a");
		Assert.assertEquals(diamond.getLines().get(1), "   b b");
		Assert.assertEquals(diamond.getLines().get(2), "  c   c");
		Assert.assertEquals(diamond.getLines().get(3), " d     d");
		Assert.assertEquals(diamond.getLines().get(4), "e       e");
		Assert.assertEquals(diamond.getLines().get(5), " d     d");
		Assert.assertEquals(diamond.getLines().get(6), "  c   c");
		Assert.assertEquals(diamond.getLines().get(7), "   b b");
		Assert.assertEquals(diamond.getLines().get(8), "    a");
	}

}