/**
 * @author JSL
 */
package com.xiaodevil.database;

import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.provider.ContactsContract.Data;
import android.util.Log;

public class DataHelper {
	private static final String TAG = "com.example.test.DataHelper";

	private static DataHelper Instance = null;
	private DataHelper() {

	}
	
	public static DataHelper getInstance(){
		if(Instance == null){
			Instance = new DataHelper();
		}
		return Instance;
	}

	/**
	 * 
	 * @param context
	 * @param na
	 * @param pho
	 */

	public void addContact(Context context, String na, String pho) {
		Log.i(TAG, "addContacts");
		Uri uri = Uri.parse("content://com.android.contacts/raw_contacts");
		ContentResolver resolver = context.getContentResolver();
		ContentValues values = new ContentValues();
		long contact_id = ContentUris.parseId(resolver.insert(uri, values));
		uri = Uri.parse("content://com.android.contacts/data");
		values.put("raw_contact_id", contact_id);
		values.put(Data.MIMETYPE, "vnd.android.cursor.item/name");
		values.put("data1", na);
		resolver.insert(uri, values);
		values.clear();
		values.put("raw_contact_id", contact_id);
		values.put(Data.MIMETYPE, "vnd.android.cursor.item/phone_v2");
		values.put("data2", "2"); 
		values.put("data1", pho);
		resolver.insert(uri, values);
		values.clear();

	}

	/**
	 * 
	 * @param context
	 * @param na
	 */
	public void deleteContact(Context context, String na) {
		Log.i(TAG, "deleteContacts");
		Uri uri = Uri.parse("content://com.android.contacts/raw_contacts");
		ContentResolver resolver = context.getContentResolver();
		Cursor cursor = resolver.query(uri, new String[] { Data._ID },
				"display_name =?", new String[] { na }, null);
		if (cursor.moveToFirst()) {
			int id = cursor.getInt(0);
			resolver.delete(uri, "display_name=?", new String[] { na });
			uri = Uri.parse("content://com.android.contacts/data");
			resolver.delete(uri, "raw_contact_id=?", new String[] { id + "" });
		}

	}

	/**
	 * 
	 * @param context
	 * @param id
	 * @param phone
	 */
	public void updateContact(Context context, int id, String phone) {
		Log.i(TAG, "updateContacts");
		Uri uri = Uri.parse("content://com.android.contacts/data");
		ContentResolver resolver = context.getContentResolver();
		ContentValues values = new ContentValues();
		values.put("data1", phone);
		resolver.update(uri, values, "mimetype=? and raw_contact_id=?",
				new String[] { "vnd.android.cursor.item/phone_v2", id + "" });
		values.clear();
	}

	/**
	 * 
	 * @param context
	 * @param na
	 * @return
	 */
	public int findContactId(Context context, String na) {
		int raw_id = -1;
		Uri uri = Uri.parse("content://com.android.contacts/raw_contacts");
		ContentResolver resolver = context.getContentResolver();
		Cursor cursor = resolver.query(uri, new String[] { Data._ID },
				"display_name=?", new String[] { na }, null);
		if (cursor.moveToFirst())
			raw_id = cursor.getInt(0);
		return raw_id;
	}

}