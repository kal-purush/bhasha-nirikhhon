package it.arduin.tables.presenter;

import android.database.sqlite.SQLiteDatabase;
import android.support.v4.app.NavUtils;
import android.view.View;
import android.widget.Toast;

import java.util.ArrayList;

import it.arduin.tables.model.ColumnSettingsHolder;
import it.arduin.tables.utils.DBUtils;
import it.arduin.tables.view.activity.CreateTableActivity;
import it.arduin.tables.view.adapter.ColumnSettingsAdapter;

/**
 * Created by Andrea on 20/05/2015.
 */
public class CreateTablePresenter {
    CreateTableActivity mActivity;

    public CreateTablePresenter(CreateTableActivity mActivity) {
        this.mActivity = mActivity;
    }

    public void onFabPressed() {
        ColumnSettingsAdapter adapter=(ColumnSettingsAdapter)mActivity.mRecyclerView.getAdapter();
        adapter.add(new ColumnSettingsHolder(""));
    }

    public void onBackButtonPressed() {
        NavUtils.navigateUpTo(mActivity,mActivity.intent);
        mActivity.finish();
    }

    public void onActionConfirmPressed() {
        createTable();
        NavUtils.navigateUpTo(mActivity,mActivity.intent);
        mActivity.finish();
    }

    private void createTable() {
        try {
            String tablename = mActivity.t.getText().toString();
            ArrayList<ColumnSettingsHolder> list = new ArrayList<>();
            for (int i = 0; i <  mActivity.mRecyclerView.getAdapter().getItemCount(); i++) {
                View v =  mActivity.mRecyclerView.getChildAt(i);
                ColumnSettingsAdapter.ViewHolder view = new ColumnSettingsAdapter.ViewHolder(v);
                ColumnSettingsHolder data = new ColumnSettingsHolder(view);
                list.add(data);
            }
            DBUtils.createTable( mActivity.path, tablename, list);
        }
        catch(Exception e){
            Toast.makeText( mActivity, e.getMessage(), Toast.LENGTH_LONG).show();
        }
    }
}