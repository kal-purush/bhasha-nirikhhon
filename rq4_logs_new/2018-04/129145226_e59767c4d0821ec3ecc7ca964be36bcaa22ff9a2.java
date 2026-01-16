package main.java.com.SQL;

import java.util.ArrayList;
import java.util.List;

import main.java.com.util.StringUtil;

public class BuildSQL {
	public static String getCreateTableString(String tableName, List<String> columnNames, boolean hasAutoIncrement) {
		String returnValue = "";
		String columns = StringUtil.toCommaSeperatedList(columnNames);
		if (hasAutoIncrement) {
			returnValue = "CREATE TABLE " + tableName.toUpperCase() + " (" + tableName.toUpperCase()+"_ID int NOT NULL AUTO_INCREMENT, " + columns + ",PRIMARY KEY ("+ tableName.toUpperCase()+"_ID)) ENGINE=InnoDB;"; //	
		} else {
			returnValue = "CREATE TABLE " + tableName.toUpperCase() + " (" + columns + ",PRIMARY KEY ("+ tableName.toUpperCase()+"_ID)) ENGINE=InnoDB;";	//
		}
		return returnValue;
	}
	
    public static String getInsertString(String tableName, List<List<String>> data, boolean hasAutoIncrement) {    	
    	StringBuilder stringBuilder = new StringBuilder("INSERT INTO " + tableName.toUpperCase() + " VALUES ");
    	if (data.size()>0) {    		    		
    		List<String> list;
    		for (int j = 0 ; j < data.size() - 1; j++) {    		
    			list = data.get(j);
    			stringBuilder.append('(');
    			if (hasAutoIncrement) {
    				stringBuilder.append("0,");    				
    			} 
    			for(int i=0; i < list.size() - 1;i++) {    				
    				stringBuilder.append("\""+list.get(i)+"\",");    				
    			}
    			stringBuilder.append("\""+list.get(list.size()-1)+"\""+"),");     			
    		}  
    		list = data.get(data.size() - 1);
			stringBuilder.append('(');
			if (hasAutoIncrement) {
				stringBuilder.append("0,");    				
			} 
			for(int i=0; i < list.size() - 1;i++) {    				
				stringBuilder.append("\""+list.get(i)+"\",");       				
			}
			stringBuilder.append("\""+list.get(list.size()-1)+"\""+");");					
    	}
    	return stringBuilder.toString();
    }
    
    public static String getInnerJoinString(List<String> joinColumn, List<String> tableNames) {
    	String returnValue = "SELECT * FROM " +tableNames.get(0);
    	for(int i = 1; i <tableNames.size();i++) {
    		returnValue = returnValue + " JOIN " + tableNames.get(i) + " ON " + tableNames.get(i -1) + "."+ joinColumn.get(i-1)+" = " + tableNames.get(i)+ "."+ joinColumn.get(i);
    	}    	
    	return returnValue;//+ " WHERE " + tableNames.get(0)+"."+joinColumn.get(0) + " IN (\"aardsda01\",\"abercda01\")" ;    	
    }
    
    public static List<String> addIndexOnTable(List<String> joinColumn, List<String> tableNames) {
    	String returnString = "";
    	List<String> returnValue = new ArrayList<String>();
    	for(int i =0; i < tableNames.size();i++) {
    		returnString = "ALTER TABLE "+ tableNames.get(i)+ " ADD INDEX (" + joinColumn.get(i)+"); ";
    		returnValue.add(returnString);
    	}    	
    	return returnValue;
    }
}