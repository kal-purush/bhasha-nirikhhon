package test.java.come.util;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import main.java.com.data.Table;
import main.java.com.file.LocalFolder;
import main.java.com.util.HibernateUtility;
import main.java.com.util.StringUtil;
import main.java.com.util.StringsToTables;

class CreateAndInsert {

	String baseballFolderPath ="";	
	String tweetFolderPath = "";
	LocalFolder baseballFolder;
	LocalFolder tweetFolder;
	
	@BeforeEach
	public void setUp() throws Exception {
		baseballFolderPath = "src//test//resources//BaseBallTestData";		
		baseballFolder = new LocalFolder(baseballFolderPath);		
		baseballFolder.loadContainedFiles("","csv");
	}	
	
	@Test
	void convertToInsert() {	
		Map<String, Table> tables = StringsToTables.convertFolderToTable(baseballFolder.getFile("Parks.csv"),',');
		Table retrievedTable = tables.get("Parks.csv");
		List<List<String>> retrievedData = retrievedTable.getTable(false).subList(1, 3);		
		assertEquals("INSERT INTO PARKS VALUES(0,\"ALT01\",\"Columbia Park\",\"\",\"Altoona\",\"PA\",\"US\"),(0,\"ANA01\",\"Angel Stadium of Anaheim\",\"Edison Field; Anaheim Stadium\",\"Anaheim\",\"CA\",\"US\")",
				StringUtil.getInsertString("Parks", retrievedData, true));
		// requers a bit too much understanding and has to be refactored. Idea would be to indicate name of table//contents/and whether it's auto ID increment
		HibernateUtility.performSQL(StringUtil.getCreateTableString("Parks", tables.get("Parks.csv").getHeaderAsString()));
		HibernateUtility.performSQL(StringUtil.getInsertString("Parks", tables.get("Parks.csv").getTable(false),true));
	}

}