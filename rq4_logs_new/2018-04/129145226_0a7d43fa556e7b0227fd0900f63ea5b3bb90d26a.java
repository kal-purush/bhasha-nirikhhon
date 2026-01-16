package main.java.com.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import main.java.com.util.HibernateUtility;
import main.java.com.util.StringUtil;


public class Database {
	
	Map<String, Table> tables = new HashMap<String, Table>();
	
	public Database() {		
	}
	
	public void addCSVData(boolean hasHeader, Map<String, List<String>> tableContents, char delimiter) {		
		for(Map.Entry<String, List<String>> entry : tableContents.entrySet()) {
			List<String> strings = entry.getValue();
			List<List<String>> stringLists = new ArrayList<List<String>>();
			for(String string: strings) {
				List<String> splitStrings = StringUtil.lineSplit(string, delimiter);
				stringLists.add(splitStrings);
			}
			Table newTable = new Table();
			newTable.setTable(stringLists, true);
			newTable.setName(entry.getKey().toUpperCase());
			tables.put(entry.getKey().toUpperCase(), newTable);			
		}			
	}
	
	public Map<String, Table> getTables(){
		return tables;
	}
	
	public void commitAll() {
		for(Map.Entry<String, Table> entry : tables.entrySet()) {
			commitTable(entry.getValue().getName());
		}
	}
	
	public void commitTable(String name) {
		Table retrievedTable = tables.get(name.toUpperCase());
		String createSQL = StringUtil.getCreateTableString(name, retrievedTable.getHeader(), true);
		String insertSQL = StringUtil.getInsertString(name, retrievedTable.getTableContents(), true);
		HibernateUtility.performDDL(createSQL);
		HibernateUtility.performDDL(insertSQL);
	}
	
	public List<String> getInnerJoin(List<String> key, List<String> tableNames, char delimiter){
		List<String> returnValue = HibernateUtility.performSelect(StringUtil.getInnerJoinString(key, tableNames), delimiter);
		returnValue.add(0,HibernateUtility.performSelect("SELECT column_name FROM information_schema.columns WHERE table_name in ('PEOPLE','APPEARANCES')",delimiter).get(0));		
		return returnValue;
	}
	
}