package it.arduin.tables;

import android.content.Intent;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.widget.Toast;

/**
 * Created by Andrea on 12/05/2015.
 */
public class MainPresenter {
    MainActivity mActivity;

    public MainPresenter(MainActivity mActivity) {
        this.mActivity = mActivity;
    }

    public void addAndSaveDatabase(String filePath,String fileName){
        DatabaseHolder d = new DatabaseHolder(fileName,filePath);
        new SharedPreferencesOperations(mActivity).addAndSaveDatabase(d.getPath(),d.getName(), mActivity.myAdapter.getItemCount());
        mActivity.myAdapter.add(d);
    }
    public void startDatabaseView(DatabaseHolder dbh){
        Intent intent = new Intent(mActivity, DatabaseViewActivity.class);
        intent.putExtra("db", dbh);
        mActivity.startActivity(intent);
    }
    public void saveDatabase(String fileName,String name){
        try{
            SQLiteDatabase db = SQLiteDatabase.openDatabase(fileName, null, SQLiteDatabase.OPEN_READWRITE);
            db.close();
            addAndSaveDatabase(fileName, name);
        }
        catch(Exception e){
            Toast.makeText(mActivity, fileName, Toast.LENGTH_LONG).show();
        }
    }
    public void createNewDatabaseAction(){
        mActivity.showNewDatabaseAlert();
    }
    public void checkAndSaveDatabase(Intent data) {
        Uri uri = data.getData();
        String filePath = DBUtils.getPath(mActivity, uri);
        if (filePath == null) filePath = uri.getPath();
        try {
            SQLiteDatabase.openDatabase(filePath, null, SQLiteDatabase.OPEN_READWRITE);
        } catch (Exception e) {
            Toast.makeText(mActivity, mActivity.getString(R.string.mainActivity_wrong_file), Toast.LENGTH_LONG).show();
        }
        String fileName = filePath.substring(1 + filePath.lastIndexOf("/"), filePath.lastIndexOf('.'));
        if (filePath.equals(""))
            Toast.makeText(mActivity, "errore, path nullo", Toast.LENGTH_LONG).show();
        else {
            addAndSaveDatabase(filePath, fileName);

        }
    }
    public void forgetDatabase(int position){
        mActivity.myAdapter.forget(position);
    }
    public void forgetAllDatabases(){
        mActivity.myAdapter.flush();
    }
    public void onDeleteAllPressed(){
        mActivity.showDeleteAllAlert();
    }
    public void startSettingsActivity(){
        Intent intent= new Intent(mActivity,TempPreferenceActivity.class);
        mActivity.startActivity(intent);
    }
    public void newDatabaseFromFileChooserAction(){
        Intent chooseFile;
        Intent intent;
        chooseFile = new Intent(Intent.ACTION_GET_CONTENT);
        chooseFile.setType("application/octet-stream");
        intent = Intent.createChooser(chooseFile, mActivity.getString(R.string.mainActivity_file_chooser_title));
        mActivity.startActivityForResult(intent, mActivity.ACTIVITY_CHOOSE_FILE);
    }
    public void newDatabaseFromPathAction(){
        mActivity.showNewDatabaseFromPathAlert();
    }
    public void onFABClick(){
        mActivity.showNewDatabasePopup();
    }

    public void onAboutPressed() {
        mActivity.showAboutAlert();
    }
}