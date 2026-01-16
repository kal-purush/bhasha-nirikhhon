package info.santhosh.evlo.data;

import android.content.Context;
import android.os.AsyncTask;

import info.santhosh.evlo.common.WriteDb;

/**
 * Created by santhoshvai on 03/02/2017.
 */

public class FavoriteAddorRemoveAsyncTask extends AsyncTask<Integer, Void, Boolean> {

    Context mContext;
    boolean mIsAdd;

    public FavoriteAddorRemoveAsyncTask(Context context, boolean isAdd) {
        mContext = context.getApplicationContext();
        mIsAdd = isAdd;
    }

    @Override
    protected Boolean doInBackground(Integer... params) {
        WriteDb writeDb = new WriteDb(mContext);
        if(mIsAdd) {
            return ( writeDb.addUsingCommoditiesFavId(params[0]) != null );
        } else {
           return ( writeDb.removeUsingCommoditiesFavId(params[0]) != 0 );
        }
    }
}