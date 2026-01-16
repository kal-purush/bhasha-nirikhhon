package com.fionera.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HardDriver {
    private static final TableName TABLE_NAME = TableName.valueOf("city");
    private static final String COLUMN_FAMILY_BASE = "base";

    private static Configuration configuration = HBaseConfiguration.create();
    private static Connection connection;

    public static void main(String[] args) throws Exception {
        createTable();
        putValue("1000", "name", "Test" + System.currentTimeMillis());
        putValue("1000", "age", "10");
        putValue("1000", "name", "Test" + System.currentTimeMillis());
        putValue("1001", "body", "Amazing " + System.currentTimeMillis());
        getValue("1000");
        getValue("1001");
//        deleteTable();
    }

    private static Connection getConnection() throws Exception {
        if (connection != null) {
            return connection;
        }
        return connection = ConnectionFactory.createConnection(configuration);
    }

    private static void createTable() throws Exception {
        Connection connection = getConnection();
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        if (admin.tableExists(TABLE_NAME)) {
            System.out.println("table exists.");
        } else {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TABLE_NAME);
            tableDescriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY_BASE));
            admin.createTable(tableDescriptor);
            System.out.println("table created.");
        }
    }

    private static void putValue(String row, String column, String value) throws Exception {
        HTable table = (HTable) getConnection().getTable(TABLE_NAME);
        Put p = new Put(Bytes.toBytes(row));
        p.addColumn(Bytes.toBytes(COLUMN_FAMILY_BASE), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(p);
        System.out.println("value put");
    }

    private static void getValue(String row) throws Exception {
        HTable table = (HTable) getConnection().getTable(TABLE_NAME);
        Get g = new Get(Bytes.toBytes(row));
        Result result = table.get(g);
        System.out.println("value got:" + result.toString());
    }

    private static void deleteTable() throws Exception {
        Connection connection = getConnection();
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        if (admin.tableExists(TABLE_NAME)) {
            try {
                admin.disableTable(TABLE_NAME);
                admin.deleteTable(TABLE_NAME);
                System.out.println("table deleted.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("table non exists.");
        }
    }
}