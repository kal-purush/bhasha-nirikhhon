package it.arduin.tables;

import android.content.Intent;
import android.database.sqlite.SQLiteDatabase;
import android.util.Log;
import android.view.View;
import android.widget.EditText;
import android.widget.TableLayout;
import android.widget.TableRow;
import android.widget.TextView;
import android.widget.Toast;

import java.util.ArrayList;
import java.util.Arrays;

import static android.database.sqlite.SQLiteDatabase.openDatabase;

/**
 * Created by Andrea on 18/05/2015.
 */
public class QuerySelectViewPresenter {
    QuerySelectViewActivity mActivity;

    public QuerySelectViewPresenter(QuerySelectViewActivity mActivity) {
        this.mActivity = mActivity;
    }
    public void onFabClick(){
        mActivity.showCustomQueryAlert();
    }

    public void openNewRecordActivity() {
        Intent intent =new Intent(mActivity,RecordAddActivity.class);
        ArrayList<String> a=new ArrayList<>(Arrays.asList(mActivity.columnNames));
        intent.putStringArrayListExtra("names", a);
        intent.putExtra("table", mActivity.table);
        intent.putExtra("path", mActivity.path);
        ArrayList<String> values= new ArrayList<>();
        for(int i=0;i<mActivity.columns;i++) values.add(a.get(i));
        intent.putStringArrayListExtra("values", values);
        mActivity.startActivityForResult(intent, QuerySelectViewActivity.NEW_RECORD);
    }
    public void onCustomQueryAction() {
        mActivity.showCustomQueryAlert();
    }

    public void onRowClick(final View v) {
        Intent intent=new Intent(mActivity,RecordViewActivity.class);
        intent.putExtra("customQuery",mActivity.customQuery);
        Log.d("columnNames", "" + mActivity.columnNames.length);
        intent.putStringArrayListExtra("names", new ArrayList<>(Arrays.asList(mActivity.columnNames)));
        intent.putExtra("table", mActivity.table);
        intent.putExtra("path", mActivity.path);
        ArrayList<String> values = new ArrayList<String>();
        for (int i = 0; i < mActivity.columns; i++) {
            ShortTextView t = (ShortTextView) ((TableRow) v).getChildAt(i);
            String text = t.getText();
            //text=text.substring(1,t.length());
            values.add(text);
        }
        intent.putExtra("values", values);
        mActivity.startActivityForResult(intent, QuerySelectViewActivity.VIEW_RECORD);
    }

    public void openCustomQuery(final EditText input) {

        Intent intent = new Intent(mActivity, QuerySelectViewActivity.class);
        intent.putExtra("query", input.getText().toString());
        intent.putExtra("table", "Query: " + input.getText().toString());
        intent.putExtra("customQuery", true);
        intent.putExtra("path", mActivity.path);
        intent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK
                | Intent.FLAG_ACTIVITY_EXCLUDE_FROM_RECENTS
                | Intent.FLAG_ACTIVITY_MULTIPLE_TASK);
        mActivity.startActivity(intent);
        // query=input.getText().toString();
        //initTable();
    }

    public void onRefreshAction() {
        mActivity.loadQuery();
    }

    public void deleteRow(final TableRow tr,View v) {
        mActivity.db = openDatabase(mActivity.path, null, SQLiteDatabase.OPEN_READWRITE);
        String sql = "DELETE FROM " + mActivity.table + " WHERE ";
        for (int i = 0; i < mActivity.columns; i++) {
            TextView t = (TextView) tr.getChildAt(i);
            String text = t.getText().toString();
            sql = sql + mActivity.columnNames[i] + "='" + text + "' AND ";
        }

        sql = sql.replace("=null", " is null");
        //sql = sql.replace("=''", " is null");
        sql=sql.substring(0,sql.lastIndexOf("AND"));
        Toast.makeText(mActivity, sql, Toast.LENGTH_LONG).show();
        try{
            mActivity.db.execSQL(sql);
            mActivity.stk.removeView(v);}
        catch(Exception e){Toast.makeText(mActivity,mActivity.getString(R.string.QuerySelectViewActivity_error_delete_record_message)+" "+e.getMessage(), Toast.LENGTH_LONG).show();}
        mActivity.db.close();
    }
}