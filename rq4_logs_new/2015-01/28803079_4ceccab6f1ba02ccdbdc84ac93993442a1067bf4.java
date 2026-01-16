package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.StringByteIterator;

import edu.ucsb.cs.mdcc.paxos.Transaction;
import edu.ucsb.cs.mdcc.paxos.TransactionException;
import edu.ucsb.cs.mdcc.txn.TransactionFactory;

public class MDCC extends DB {

	final TransactionFactory fac = new TransactionFactory();
	
	@Override
	public int delete(String tableName, String key) {
		if (tableName == null) {
			return -1;
		}
		if (key == null) {
			return -1;
		}
		Transaction t = fac.create();
        t.begin();
        StringBuilder str = new StringBuilder(tableName);
        str.append("_");
        str.append(key);
        String theKey = str.toString();
		try {
			t.delete(theKey);
			t.commit();
		} catch (TransactionException e) {
			System.err.println("Error in processing delete to table: " + tableName + e);
			return -1;
		}
		return 0;
	}

	@Override
	public int insert(String tableName, String key, HashMap<String, ByteIterator> values) {
		if (tableName == null) {
			return -1;
		}
		if (key == null) {
			return -1;
		}
		Transaction t = fac.create();
        t.begin();
        StringBuilder str = new StringBuilder(tableName);
        str.append("_");
        str.append(key);
        String theKey = str.toString();
		try {
			for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
				t.write(theKey, entry.getValue().toString().getBytes());
			}
			t.commit();
		} catch (TransactionException e) {
			System.err.println("Error in processing insert to table: " + tableName + e);
			return -1;
		}
		return 0;
	}

	@Override
	public int read(String tableName, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		if (tableName == null) {
			return -1;
		}
		if (key == null) {
			return -1;
		}
		Transaction t = fac.create();
        t.begin();
        StringBuilder str = new StringBuilder(tableName);
        str.append("_");
        str.append(key);
        String theKey = str.toString();
		try {
			for (String field : fields) {
				byte[] x = t.read(theKey);
				result.put(field, new StringByteIterator(x.toString()));
			}
			t.commit();
		} catch (TransactionException e) {
			System.err.println("Error in processing insert to table: " + tableName + e);
			return -1;
		}
		return 0;
	}

	@Override
	public int scan(String arg0, String arg1, int arg2, Set<String> arg3,
			Vector<HashMap<String, ByteIterator>> arg4) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int update(String tableName, String key, HashMap<String, ByteIterator> values) {
		if (tableName == null) {
			return -1;
		}
		if (key == null) {
			return -1;
		}
		Transaction t = fac.create();
        t.begin();
        StringBuilder str = new StringBuilder(tableName);
        str.append("_");
        str.append(key);
        String theKey = str.toString();
		try {
			for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
				t.write(theKey, entry.getValue().toString().getBytes());
			}
			t.commit();
		} catch (TransactionException e) {
			System.err.println("Error in processing insert to table: " + tableName + e);
			return -1;
		}
		return 0;
	}

}