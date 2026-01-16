package io.c12.bala.datacopy;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ResourceBundle;
/**
 * @author palanib2
 * 
 * To drop all the tables in a schema -  DROP SCHEMA if exists public.public cascade;
 * 
 * This program copies the table structure from source system and then all the primary key and foreign keys.
 * Tables, which need to be copied are in tables.txt and domain table names are in domainTableList.txt
 */
public class CopyTableAndData {
	
	private static final String SPACE = " ";
	private static final String UNDERSCORE = "_";
	private static final String COMMAN = ",";
	// TODO: Make sure the values are correct for the DB its used
	private static final Integer[] SIZE_REQUIRED = {-4, -3, -2, -1, 1, 12, 2004, 2005};
	private static final Integer[] SIZE_DECIMAL_REQUIRED = {3, 8};
	private static final int BATCH_INSERT_SIZE = 1000;
	private static final boolean COPY_DATA = false;
	private static final boolean COPY_DOMAIN_TABLES_ONLY = true;

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		long startTime = System.currentTimeMillis();
		System.out.println(new Date());
		ResourceBundle resource  = ResourceBundle.getBundle("application");
		List<String> tableNames = getTableName();
		String schemaName = resource.getString("jdbc.db2.schema");
		
		Connection hyperSqlConn = getHyperSqlConnection(resource);
		Connection db2Conn = getDB2Connection(resource);
		
		// -- Step 1: Create table and add PK constraints
		for (String tableName : tableNames) {
			System.out.println("-- " + tableName);
			String createTableQuery = createTableQuery(db2Conn, tableName);
			executeUpdate(hyperSqlConn, createTableQuery);
			String primaryKeyConstraintQuery = createPkConstraintQuery(db2Conn, tableName, schemaName);
			executeUpdate(hyperSqlConn, primaryKeyConstraintQuery);
		}
		
		// -- Step 2: copy data using insert
		if (COPY_DATA) {
			System.out.println("-- Copy data");
			copyData(db2Conn, hyperSqlConn, tableNames);
		}
		
		// -- Step 2.1: copy domain table data using insert
		if (COPY_DOMAIN_TABLES_ONLY) {
			System.out.println("-- Copy Domain data");
			List<String> domainTalbes = getDomainTableNames();
			copyData(db2Conn, hyperSqlConn, domainTalbes);
		}
		
		System.out.println("-- Create FK constraints");
		// -- Step 3: Create FK constraints after tables are created.
		for (String tableName : tableNames) {
			List<String> foreignKeyQueries = createFkConstraintQuery(db2Conn, tableName, schemaName, tableNames);
			for (String foreginKeyQuery : foreignKeyQueries) {
				System.out.println(foreginKeyQuery);
				executeUpdate(hyperSqlConn, foreginKeyQuery);
			}
		}
		
		System.out.println("-- Identity setup");
		// -- Step 4: Add autoIncrement after data moved
		for (String tableName : tableNames) {
			String addIdentiyQuery = createIdentityColumns(db2Conn, tableName);
			if (addIdentiyQuery != null && !addIdentiyQuery.isEmpty()) {
				System.out.println(addIdentiyQuery);
				executeUpdate(hyperSqlConn, addIdentiyQuery);
				
				// -- Step 5: Update identity restart value
				String updateIdentityValueQuery = udpateIdentityValue(hyperSqlConn, tableName);
				if (updateIdentityValueQuery != null && !updateIdentityValueQuery.isEmpty()) {
					System.out.println(updateIdentityValueQuery);
					executeUpdate(hyperSqlConn, updateIdentityValueQuery);
				}
			}
		}

		closeConnection(db2Conn);
		closeConnection(hyperSqlConn);
		System.out.println("Time taken " + (System.currentTimeMillis() - startTime) + "ms");
	}
	
	/**
	 * @param connection
	 * @param tableName
	 * @return Query to create table
	 * @throws SQLException
	 * 
	 * Create query to create table in target DB
	 */
	private static final String createTableQuery(Connection connection, String tableName) throws SQLException {
		PreparedStatement prepStmt = connection.prepareStatement("SELECT * FROM " + tableName + " where 1=2");
		ResultSet rs = prepStmt.executeQuery();
		ResultSetMetaData rsMeta = rs.getMetaData();
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE TABLE " + tableName + " ( ");
		for(int i = 1 ; i <= rsMeta.getColumnCount() ; i++ ) {
			sb.append(rsMeta.getColumnName(i) + SPACE );
			// For varbinary used only in bank_acct_info
			if(rsMeta.getColumnType(i) == -3) {
				sb.append("VARBINARY(" + rsMeta.getPrecision(i) + ")");
			} else if (Arrays.asList(SIZE_REQUIRED).contains(rsMeta.getColumnType(i))) {
				sb.append(rsMeta.getColumnTypeName(i));
				sb.append("(" + rsMeta.getPrecision(i) + ")");
			} else if (Arrays.asList(SIZE_DECIMAL_REQUIRED).contains(rsMeta.getColumnType(i))) {
				sb.append(rsMeta.getColumnTypeName(i));
				sb.append("(" + rsMeta.getPrecision(i) + COMMAN + rsMeta.getScale(i) + ")");
			} else {
				sb.append(rsMeta.getColumnTypeName(i));
			}
			sb.append(rsMeta.isNullable(i) == 0 ? " NOT NULL, " : ", ");
		}
		String createQuery = sb.toString().substring(0, sb.toString().length() -2) + " )";
		System.out.println(createQuery);
		rs.close();
		prepStmt.close();
		return createQuery;
	}
	
	/**
	 * @param connection
	 * @param tableName
	 * @param schemaName
	 * @return Query to add Primary key to copied over table
	 * @throws SQLException
	 * 
	 * Create a primary key constraints for the tables copied over to target DB
	 */
	private static final String createPkConstraintQuery(Connection connection, String tableName, String schemaName) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE " + tableName + " ADD CONSTRAINT PK_" + tableName + " PRIMARY KEY (");
		// -- Primary Key
		DatabaseMetaData dm = connection.getMetaData();
		ResultSet rset = dm.getPrimaryKeys(null, schemaName, tableName);
		while( rset.next( ) ) {    
			String pkey = rset.getString(4);
			sb.append(pkey + ", ");
		}
		String primaryKeyConstraintQuery = sb.toString().substring(0, sb.toString().length() -2) + ")";
		System.out.println(primaryKeyConstraintQuery);
		rset.close();
		return primaryKeyConstraintQuery;
	}
	
	/**
	 * @param connection
	 * @param tableName
	 * @param schemaName
	 * @param tableList
	 * @return Query to add Foreign Key Constraint
	 * @throws SQLException
	 * 
	 * Creates a Foreign key constraints for the tables copied over to target DB
	 */
	private static final List<String> createFkConstraintQuery(Connection connection, String tableName, String schemaName, List<String> tableList) throws SQLException {
		DatabaseMetaData dm = connection.getMetaData();
		ResultSet rset = dm.getImportedKeys(null, schemaName, tableName);
		List<String> foreignKeyQueries = new ArrayList<String>();
		
		while (rset.next()) {
			if (tableList.contains(rset.getString(3))) {
				StringBuilder sb = new StringBuilder();
				sb.append("ALTER TABLE " + tableName + " ADD CONSTRAINT FK_");
				sb.append(rset.getString(3) + UNDERSCORE + rset.getString(7));
				sb.append(" FOREIGN KEY (");
				sb.append(rset.getString(8) + ") REFERENCES ");
				sb.append( rset.getString(3) + "(" + rset.getString(4) + ")");
				foreignKeyQueries.add(sb.toString());
			}
		}
		rset.close();
		return foreignKeyQueries;
	}
	
	/**
	 * @param srcConn
	 * @param destConn
	 * @param tableNameList
	 * 
	 * Copy data from Source DB table to target table
	 */
	private static final void copyData(Connection srcConn, Connection destConn, List<String> tableNameList) {
		try {
			for( String tableName : tableNameList) {
				String sourceSelectQuery = "SELECT * FROM " + tableName;
				PreparedStatement srcPrepStmt = srcConn.prepareStatement(sourceSelectQuery);
				
				ResultSet srcRs = srcPrepStmt.executeQuery();
				ResultSetMetaData srcRsMd = srcPrepStmt.getMetaData();
				
				destConn.setAutoCommit(false);
				String destSelectQuery = "SELECT * FROM " + tableName + " WHERE 1=2";
				PreparedStatement destPrepStmt = destConn.prepareStatement(destSelectQuery);
				ResultSet destRs = destPrepStmt.executeQuery();
				ResultSetMetaData destRsMd = destPrepStmt.getMetaData();
				
				if (destRsMd.getColumnCount() != srcRsMd.getColumnCount()) {
					throw new IllegalArgumentException(tableName + " - number of coulmns do not match");
				}
				
				HashMap<String, Integer> destColumnNames = getColumnNames(destRsMd);
				
				List<String> columnNames = new ArrayList<String>();
				List<String> columnParams = new ArrayList<String>();
	
				for (int i = 1; i <= srcRsMd.getColumnCount(); i++) {
					String srcCoulmnName = srcRsMd.getColumnName(i);
					if (destColumnNames.get(srcCoulmnName) == null) {
						throw new IllegalArgumentException("could not find " + srcCoulmnName + " in destination table " + tableName);
					}
					columnNames.add(srcCoulmnName);
					columnParams.add("?");
				}
				
				String columns = columnNames.toString().replace('[', '(').replace(']', ')');
				String params = columnParams.toString().replace('[', '(').replace(']', ')');
	
				String destInsertSql = "INSERT INTO " + tableName + " " + columns + " VALUES " + params;
				System.out.println(destInsertSql);
				
				PreparedStatement destInsertStmt = destConn.prepareStatement(destInsertSql);
				
				int count = 0;
				while (srcRs.next()) {
					for (int i = 1; i <= srcRsMd.getColumnCount(); i++) {
						destInsertStmt.setObject(i, srcRs.getObject(i));
					}
					destInsertStmt.addBatch();
					try {
						if(++count % BATCH_INSERT_SIZE == 0) {
							destInsertStmt.executeBatch();
						}
					} catch (SQLException e) {
						// Assuming the PK is first column
						System.err.println("Skipping row where " + srcRsMd.getColumnName(1) + " = " + srcRs.getObject(1) + " because of " + e.getMessage());
					}
				}
				if (count % BATCH_INSERT_SIZE != 0){
					destInsertStmt.executeBatch();
				}
				destInsertStmt.close();
				destRs.close();
				destPrepStmt.close();
				srcRs.close();
				srcPrepStmt.close();
			}
			destConn.commit();
		} catch (SQLException e1) {
			e1.printStackTrace();
			try {
				destConn.rollback();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * @param db2Conn
	 * @param tableName
	 * @return Query to create identity for PK
	 * @throws SQLException
	 * 
	 * If the Source Table have Identity, its creates a identity in target table
	 */
	private static final String createIdentityColumns(Connection db2Conn, String tableName) throws SQLException {
		PreparedStatement prepStmt = db2Conn.prepareStatement("SELECT * FROM " + tableName + " where 1=2");
		ResultSet rs = prepStmt.executeQuery();
		ResultSetMetaData rsMeta = rs.getMetaData();
		StringBuilder sb = new StringBuilder();
		for(int i = 1 ; i <= rsMeta.getColumnCount() ; i++ ) {
			if (rsMeta.isAutoIncrement(i)) {
				sb.append("ALTER TABLE " + tableName + " ALTER COLUMN " + rsMeta.getColumnName(i) + " identity");
			}
		}
		rs.close();
		prepStmt.close();
		return sb.toString();
	}
	
	/**
	 * @param hyperSqlConn
	 * @param tableName
	 * @return Query to Update Identity value for newly created tables
	 * @throws SQLException
	 * 
	 * Get the latest primary key value and update identity base value to Max of key +1
	 */
	private static final String udpateIdentityValue(Connection hyperSqlConn, String tableName) throws SQLException {
		PreparedStatement prepStmt = hyperSqlConn.prepareStatement("SELECT * FROM " + tableName + " where 1=2");
		ResultSet rs = prepStmt.executeQuery();
		ResultSetMetaData rsMeta = rs.getMetaData();
		StringBuilder sb = new StringBuilder();
		
		// -- get column which is identity
		for(int i = 1 ; i <= rsMeta.getColumnCount() ; i++ ) {
			if (rsMeta.isAutoIncrement(i)) {
				String getMaxIdentityValQuery = "SELECT MAX("+ rsMeta.getColumnName(i) +") FROM " + tableName;
				System.out.println(getMaxIdentityValQuery);
				PreparedStatement idenPrepStmt = hyperSqlConn.prepareStatement(getMaxIdentityValQuery);
				ResultSet idenRs = idenPrepStmt.executeQuery();
				int maxIdentityCount = 0;
				while (idenRs.next()){
					maxIdentityCount = idenRs.getInt(1);
				}
				idenRs.close();
				idenPrepStmt.close();
				if (maxIdentityCount > 0) {
					sb.append("ALTER TABLE " + tableName + " ALTER COLUMN " + rsMeta.getColumnName(i) );
					sb.append(" RESTART WITH " + (maxIdentityCount + 1));
				}
			}
		}
		rs.close();
		prepStmt.close();
		return sb.toString();
	}
	
	/**
	 * @param metaData
	 * @return Hashmap of column and number
	 * @throws SQLException
	 * 
	 * Get columns names from ResultSet Meta Data
	 */
	private static HashMap<String, Integer> getColumnNames(ResultSetMetaData metaData) throws SQLException {
		HashMap<String, Integer> columnNames = new HashMap<String, Integer>();
		for (int i = 1; i <= metaData.getColumnCount(); i++) {
			columnNames.put(metaData.getColumnName(i), i);
		}
		return columnNames;
	}
	
	/**
	 * @return List of table names
	 * @throws IOException
	 * 
	 * Get list of table names to be migrated from text file. One table per line.
	 */
	private static final List<String> getTableName() throws IOException {
		Path path = FileSystems.getDefault().getPath("src/main/resources", "tables.txt");
		return Files.readAllLines(path);
	}
	
	/**
	 * @return List of domain table names
	 * @throws IOException
	 * 
	 * Get all domain table names from properties file
	 */
	private static final List<String> getDomainTableNames() throws IOException {
		Path path = FileSystems.getDefault().getPath("src/main/resources", "domainTableList.txt");
		return Files.readAllLines(path);
	}
	
	/**
	 * @param connection
	 * @param query
	 * @throws SQLException
	 * 
	 * Execute update query
	 */
	private static final void executeUpdate(Connection connection, String query) throws SQLException {
		PreparedStatement prepStmt = connection.prepareStatement(query);
		prepStmt.executeUpdate();
		prepStmt.close();
	}
	
	/**
	 * @param resource
	 * @return DB2 Connection
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * 
	 * Create DB2 Connection using connection parameters from properties file
	 */
	private static final Connection getDB2Connection(ResourceBundle resource) throws SQLException, ClassNotFoundException {
		Class.forName( resource.getString("jdbc.source.driver.classname"));
		Connection db2Conn = DriverManager.getConnection(resource.getString("jdbc.source.url"), resource.getString("jdbc.source.userid"), resource.getString("jdbc.source.password"));
		return db2Conn;
	}
	
	/**
	 * @param resource
	 * @return HyperSQL in-memory database connection
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * 
	 * Create HyperSQL in-memory DB Connection using connection parameters from properties file
	 */
	private static final Connection getHyperSqlConnection(ResourceBundle resource) throws ClassNotFoundException, SQLException {
		Class.forName( resource.getString("jdbc.target.driver.classname"));
		Connection hyperSqlConn = DriverManager.getConnection(resource.getString("jdbc.target.url"), resource.getString("jdbc.target.userid"), resource.getString("jdbc.target.password"));
		return hyperSqlConn;
	}
	
	/**
	 * @param connection
	 * @throws SQLException
	 * 
	 * Close DB connection
	 */
	private static final void closeConnection(Connection connection) throws SQLException {
		connection.close();
	}

}