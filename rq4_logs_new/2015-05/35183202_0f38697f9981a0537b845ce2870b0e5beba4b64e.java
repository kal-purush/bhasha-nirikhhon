package it.arduin.tables;

import android.content.Context;
import android.content.Intent;
import android.database.sqlite.SQLiteDatabase;
import android.support.v7.widget.LinearLayoutManager;
import android.util.Log;
import android.view.View;
import android.widget.Toast;

import java.util.ArrayList;

/**
 * Created by Andrea on 15/05/2015.
 */
public class DatabaseViewPresenter {
    DatabaseViewActivity mActivity;
    public DatabaseViewPresenter(DatabaseViewActivity databaseViewActivity){
        mActivity=databaseViewActivity;
    }

    public void onFabClick() {
        Intent intent=new Intent(mActivity,CreateTableActivity.class);
        intent.putExtra("path",mActivity.path);
        intent.putExtra("name",mActivity.toolbar.getTitle()+",new table");
        mActivity.startActivityForResult(intent, 1);
    }

    public void onRecyclerItemPressed(View view, int position) {
        Intent intent = new Intent(mActivity, QuerySelectViewActivity.class);
        String query = "SELECT ";
        ArrayList<ColumnPair> definitions = DBUtils.getColumns(mActivity.path, mActivity.mAdapter.getItemAt(position).getName());
        String[] columnTypes = ColumnPair.getTypeArray(definitions);
        String[] columnNames = ColumnPair.getNameArray(definitions);
        int columns = columnNames.length;
        for (int i = 0; i < columns; i++) {
            if (columnTypes[i].toLowerCase().trim().equals("blob"))
                query += "quote(" + columnNames[i] + ")";
            else query += columnNames[i];
            if (i != columns - 1) query += ",";
        }
        query += " FROM " + mActivity.mAdapter.getItemAt(position).getName();
        query += " LIMIT " + SettingsActivity.getQueryLimit(mActivity);
        Log.d("qry", query);
        intent.putExtra("query", query);
        intent.putExtra("table", mActivity.mAdapter.getItemAt(position).getName());
        intent.putExtra("customQuery", false);
        intent.putExtra("path", mActivity.path);
        // ((dataContainer)getApplication()).db=db;
        mActivity.startActivity(intent);
    }

    public void onActionInfoPressed() {
        Intent intent=new Intent(mActivity,DatabaseInfoActivity.class);
        intent.putExtra("db",mActivity.dbh);
        intent.putExtra("path",mActivity.path);
        mActivity.startActivityForResult(intent, mActivity.DATABASE_INFO);
    }

    public void onDeleteOptionPressed(String name, int position) {
        mActivity.showDeleteTableAlert(name, position);
    }

    public void deleteTable(String name, int position) {
        try{
            String sql="DROP TABLE IF EXISTS '"+name+"'";
            //Toast.makeText(DatabaseView.c,sql,Toast.LENGTH_LONG).show();
            SQLiteDatabase db=SQLiteDatabase.openDatabase(mActivity.path,null,SQLiteDatabase.OPEN_READWRITE);
            db.execSQL(sql);
            db.close();
            mActivity.mAdapter.remove(position);
        }
        catch (Exception e){
            Toast.makeText(mActivity, e.getMessage(), Toast.LENGTH_LONG).show();
        }
    }

    public void onActivityResult() {
        mActivity.showReloadAlert();
    }

    public void reloadTables() {
        mActivity.mAdapter = new DatabaseViewAdapter(new ArrayList<TableHolder>(), mActivity);
        mActivity.mRecyclerView.setAdapter( mActivity.mAdapter);
        mActivity.mRecyclerView.setLayoutManager(new LinearLayoutManager( mActivity));
        mActivity.mAdapter.empty();
        mActivity.loadTables();
    }

    public void onRenameOptionPressed(String name, int position) {
        mActivity.showRenameAlert(name, position);
    }

    public void renameTable(String name, int position, String input) {
        try {
            SQLiteDatabase db;
            db = SQLiteDatabase.openDatabase( mActivity.path, null, SQLiteDatabase.CREATE_IF_NECESSARY);
            db.execSQL("ALTER TABLE '" + name + "' RENAME TO '" + input + "'");
            TableHolder t = mActivity.mAdapter.list.get(position);
            t = new TableHolder(input, t.getFields(),  mActivity.dbh);
            mActivity.mAdapter.list.set(position, t);
            mActivity.mAdapter.notifyDataSetChanged();

            //Toast.makeText(c,input,Toast.LENGTH_LONG).show();
        } catch (Exception e) {
            Toast.makeText( mActivity, e.getMessage(), Toast.LENGTH_LONG).show();
        }
    }

    public void viewTableInfo(String name, int position, Context c) {
        Intent intent=new Intent(c,QuerySelectViewActivity.class);
        String table=name;
        intent.putExtra("query", "PRAGMA table_info('" + table + "')");
        intent.putExtra("table", table);
        intent.putExtra("customQuery", true);
        intent.putExtra("path", mActivity.path);
        mActivity.startActivity(intent);
    }
}