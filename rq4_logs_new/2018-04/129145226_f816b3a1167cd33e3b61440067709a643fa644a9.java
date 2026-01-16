package test.java.com.files;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import main.java.com.files.DataStatus;
import main.java.com.files.Table;

class TableTest {
	
	Table table = new Table();
	
	@BeforeEach
	public void setUp() throws Exception {
		List<String> header = Arrays.asList("Column1", "Column2", "Column3");
		List<String> row1 = Arrays.asList("1", "2", "3");
		List<String> row2 = Arrays.asList("4", "5", "6");
		List<String> row3 = Arrays.asList("7", "8", "9");
		List<String> row4 = Arrays.asList("10", "11", "12","13");
		List<String> row5 = Arrays.asList("14", "15");
		List<List<String>> table1 = new ArrayList<List<String>>();
		table1.add(header);
		table1.add(row1);
		table1.add(row2);
		table1.add(row3);
		table1.add(row4);
		table1.add(row5);
		table.setTable(table1, true);		
	}	
	
	@Test
	@DisplayName("Test initial row status")
	void testRowStatus() {
		assertEquals(table.getRowStatus(0),DataStatus.UNCHANGED_OK);
		assertEquals(table.getRowStatus(1),DataStatus.UNCHANGED_OK);
		assertEquals(table.getRowStatus(2),DataStatus.UNCHANGED_OK);
		assertEquals(table.getRowStatus(3),DataStatus.UNCHANGED_OK);
		assertEquals(table.getRowStatus(4),DataStatus.UNCHANGED_OK);
	}
	
	@Test
	@DisplayName("Test get table")
	void testContentValues() {
		List<List<String>> tableContents = table.getTable();
		assertEquals(tableContents.get(1).get(1),"2");
		assertEquals(tableContents.get(5).get(1),"15");
		assertEquals(tableContents.get(2).get(2),"6");		
	}
	
	@Test
	@DisplayName("Test get column")
	void getColumn() {
		List<String> column = table.getColumn(0);
		assertEquals(column.get(0),"1");
		assertEquals(column.get(1),"4");
		assertEquals(column.get(2),"7");
		assertEquals(column.get(3),"10");	
		assertEquals(column.get(4),"14");
		column = table.getColumn(1);
		assertEquals(column.get(0),"2");
		assertEquals(column.get(1),"5");
		assertEquals(column.get(2),"8");
		assertEquals(column.get(3),"11");	
		assertEquals(column.get(4),"15");
		column = table.getColumn(3);
		assertNull(column.get(4));
		column = table.getColumn(3);
		assertNull(column.get(0));		
		assertEquals(column.get(3),"13");
	}
	
	@Test
	@DisplayName("Get row")
	void getRow() {
		List<String> row = table.getRow(0);
		assertEquals(row.get(0),"1");
		assertEquals(row.get(1),"2");
		assertEquals(row.get(2),"3");
		row = table.getRow(3);
		assertEquals(row.get(3),"13");
		row = table.getRow(4);
		assertEquals(row.get(1),"15");	
	}
	
	@Test
	@DisplayName("Enforce header size")
	void setHeaderSize() {
		assertEquals(table.getRow(3).get(3),"13");
		assertThrows(IndexOutOfBoundsException.class,				
				()->{table.getRow(4).get(2);});
		assertEquals(table.getRowStatus(1),DataStatus.UNCHANGED_OK);
		assertEquals(table.getRowStatus(3),DataStatus.UNCHANGED_OK);
		assertEquals(table.getRowStatus(4),DataStatus.UNCHANGED_OK);
		table.applyHeaderSize();		
		assertThrows(IndexOutOfBoundsException.class,				
				()->{table.getRow(3).get(3);});
		assertNull(table.getRow(4).get(2));
		assertEquals(table.getRowStatus(1),DataStatus.UNCHANGED_OK);
		assertEquals(table.getRowStatus(3),DataStatus.FAULTY);
		assertEquals(table.getRowStatus(4),DataStatus.FAULTY);
		List<List<String>> tableContents = table.getTable();
		assertEquals(tableContents.size(),6);
		assertEquals(tableContents.get(5).size(),3);
	}
	
	@Test
	@DisplayName("Update value")
	void updateValues() {
		assertEquals(table.getRowStatus(1),DataStatus.UNCHANGED_OK);
		table.updateValue(1, 1, "temp", false);
		List<String> column = table.getColumn(1);
		assertEquals(column.get(1),"temp");
		List<String> row = table.getRow(1);
		assertEquals(row.get(1),"temp");
		assertEquals(table.getRowStatus(1),DataStatus.CHANGED_NOK);
		assertEquals(table.getRowStatus(2),DataStatus.UNCHANGED_OK);
		table.updateValue(4, 3, "temp", true);
		column = table.getColumn(3);
		row = table.getRow(4);
		assertEquals(row.get(3),"temp");
		assertEquals(column.get(4),"temp");
	}

}