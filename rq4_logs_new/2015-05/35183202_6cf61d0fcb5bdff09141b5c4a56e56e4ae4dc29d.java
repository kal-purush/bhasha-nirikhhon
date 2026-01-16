package it.arduin.tables;

/**
 * Created by Andrea on 14/05/2015.
 */
public class DatabaseInfoPresenter {
    DatabaseInfoActivity mActivity;

    public DatabaseInfoPresenter(DatabaseInfoActivity mActivity) {
        this.mActivity = mActivity;
    }

    public void onFabClicked() {
        mActivity.showDeletePopup();
    }

    public void deleteDatabase() {
        try {
            mActivity.dbh.delete();
        }
        catch (Exception e){
            mActivity.showError(e);
        }
    }
}