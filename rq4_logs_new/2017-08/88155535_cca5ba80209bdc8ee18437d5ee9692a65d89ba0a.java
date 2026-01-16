package com.acmr.excel.test;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import acmr.excel.pojo.Constants.CELLTYPE;
import acmr.excel.pojo.ExcelCell;

import com.acmr.excel.model.complete.Content;
import com.acmr.excel.model.complete.Format;
import com.acmr.excel.util.CellFormateUtil;

public class CellFormatTest {
	private ExcelCell excelCell;
	private Content content;
	private Format formate;

	@Before
	public void before() {
		excelCell = new ExcelCell();
		content = new Content();
		formate = new Format();
	}

	/**
	 * 测试dataformat=0,text为普通数字
	 */
	@Test
	public void testDataFormatewith0Number() {
		excelCell.getCellstyle().setDataformat("0");
		excelCell.setType(CELLTYPE.NUMERIC);
		excelCell.setText("31526445.520896237");
		CellFormateUtil.setShowText(excelCell, content, formate);
		Assert.assertEquals("31526445.520896237", content.getTexts());
		Assert.assertEquals("31526446", content.getDisplayTexts());
		Assert.assertEquals("number", formate.getType());
		Assert.assertEquals(Integer.valueOf(0), formate.getDecimal());
	}

	/**
	 * 测试dataformat=0,text为科学计数法
	 */
	@Test
	public void testDataFormatewith0SinceNumber() {
		excelCell.getCellstyle().setDataformat("0");
		excelCell.setType(CELLTYPE.NUMERIC);
		excelCell.setText("3.1526445520896237E7");
		CellFormateUtil.setShowText(excelCell, content, formate);
		Assert.assertEquals("31526445.520896237", content.getTexts());
		Assert.assertEquals("31526446", content.getDisplayTexts());
		Assert.assertEquals("number", formate.getType());
		Assert.assertEquals(Integer.valueOf(0), formate.getDecimal());
	}
	/**
	 * 测试dataformat=0,text为文本
	 */
	@Test
	public void testDataFormatewith0Text() {
		excelCell.getCellstyle().setDataformat("0");
		excelCell.setType(CELLTYPE.STRING);
		excelCell.setText("文本内容");
		CellFormateUtil.setShowText(excelCell, content, formate);
		Assert.assertEquals("文本内容", content.getTexts());
		Assert.assertEquals("文本内容", content.getDisplayTexts());
		Assert.assertEquals(Boolean.valueOf(false), formate.getIsValid());
		Assert.assertEquals("number", formate.getType());
		Assert.assertEquals(Integer.valueOf(0), formate.getDecimal());
	}
	/**
	 * 测试小数位数问题(git #310),1位小数
	 * 问题描述
		原excel设置单元格格式为数值格式，小数位数为2位，在文件上传后，所有设置两位小数的单元格均显示为一位小数。双击该单元格，显示的也为一位小数，未将原始表中数值全都显示出来。
		与此同时，对小数位数为0位、1位、2位、3位、4位、常规格式进行了测试，常规格式、0位小数位数显示正常，双击单元格正常显示原数值，1位小数显示正常，双击无法正常显示原数值，
		2-4位小数显示不正常，双击无法正确显示原数值。
	 */
	@Test
	public void testDecimalDigitWith2(){
		excelCell.getCellstyle().setDataformat("0.00_ ");
		excelCell.setType(CELLTYPE.NUMERIC);
		excelCell.setText("2.0032");
		excelCell.setValue(2.0032);
		CellFormateUtil.setShowText(excelCell, content, formate);
		Assert.assertEquals("2.0032", content.getTexts());
		Assert.assertEquals("2.00", content.getDisplayTexts());
		Assert.assertEquals("number", formate.getType());
		Assert.assertEquals(Integer.valueOf(2), formate.getDecimal());
	}
	/**
	 * 测试小数位数问题(git #310),3位小数
	 */
	@Test
	public void testDecimalDigitWith3(){
		excelCell.getCellstyle().setDataformat("0.000_ ");
		excelCell.setType(CELLTYPE.NUMERIC);
		excelCell.setText("2.45563");
		excelCell.setValue(2.45563);
		CellFormateUtil.setShowText(excelCell, content, formate);
		Assert.assertEquals("2.45563", content.getTexts());
		Assert.assertEquals("2.456", content.getDisplayTexts());
		Assert.assertEquals("number", formate.getType());
		Assert.assertEquals(Integer.valueOf(3), formate.getDecimal());
	}
	
	/**
	 * 测试小数位数问题(git #310),4位小数
	 */
	@Test
	public void testDecimalDigitWith4(){
		excelCell.getCellstyle().setDataformat("0.0000_ ");
		excelCell.setType(CELLTYPE.NUMERIC);
		excelCell.setText("2.45563");
		excelCell.setValue(2.45563);
		CellFormateUtil.setShowText(excelCell, content, formate);
		Assert.assertEquals("2.45563", content.getTexts());
		Assert.assertEquals("2.4556", content.getDisplayTexts());
		Assert.assertEquals("number", formate.getType());
		Assert.assertEquals(Integer.valueOf(4), formate.getDecimal());
	}
	/**
	 * 测试小数位数问题(git #310),1位小数
	 */
	@Test
	public void testDecimalDigitWith1(){
		excelCell.getCellstyle().setDataformat("0.0_);[Red]\\(0.0\\)");
		excelCell.setType(CELLTYPE.NUMERIC);
		excelCell.setText("-2.4553");
		excelCell.setValue(-2.4553);
		CellFormateUtil.setShowText(excelCell, content, formate);
		Assert.assertEquals("-2.4553", content.getTexts());
		Assert.assertEquals("-2.5", content.getDisplayTexts());
		Assert.assertEquals("number", formate.getType());
		Assert.assertEquals(Integer.valueOf(1), formate.getDecimal());
	}
	/**
	 * git #309 问题描述

	原excel显示正常，在文件上传后，能够正常看到显示的文本（如C3单元格），双击之后不显示文本内容。该单元格可以进行二次编辑。二次编辑后的内容可以保存。

	部分单元格双击不能显示。如该表，行单元格双击后无法正常显示。列单元格双击后可以正常显示。未找到单元格规律
	 */
	@Test
	public void testText(){
		excelCell.getCellstyle().setDataformat("0_);[Red]\\(0\\)");
		excelCell.setType(CELLTYPE.STRING);
		excelCell.setText("累计");
		excelCell.setValue("累计");
		CellFormateUtil.setShowText(excelCell, content, formate);
		Assert.assertEquals("累计", content.getTexts());
		Assert.assertEquals("累计", content.getDisplayTexts());
		Assert.assertEquals("number", formate.getType());
		Assert.assertEquals(Integer.valueOf(0), formate.getDecimal());
		Assert.assertEquals(Boolean.valueOf(false), formate.getIsValid());
		Assert.assertEquals(Boolean.valueOf(false), formate.getThousands());
	}
	
	
	
	
	
	
	
}