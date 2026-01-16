package com.acmr.excel.test;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.acmr.excel.model.complete.Content;

import acmr.excel.pojo.ExcelFont;

/**
 * 测试重新打开
 * 
 * @author jinhr
 *
 */
public class PositionTest {
	private ExcelFont excelFont;
	private Content content;

	@Before
	public void before() {
		excelFont = new ExcelFont();
		content = new Content();
	}

	/**
	 * 测试下划线的重新打开
	 */
	@Test
	public void testPositionWithUnderline() {
		excelFont.setUnderline((byte) 0);
		content.setUnderline(excelFont.getUnderline() == 0 ? "0" : "1");
		Assert.assertEquals("0", content.getUnderline());
		excelFont.setUnderline((byte) 1);
		content.setUnderline(excelFont.getUnderline() == 0 ? "0" : "1");
		Assert.assertEquals("1", content.getUnderline());
		excelFont.setUnderline((byte) 2);
		content.setUnderline(excelFont.getUnderline() == 0 ? "0" : "1");
		Assert.assertEquals("1", content.getUnderline());
	}

	@After
	public void after() {
		excelFont = null;
		content = null;
	}
}