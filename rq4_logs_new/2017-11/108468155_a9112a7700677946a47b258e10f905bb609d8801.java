package aaron.filecommand.activity;

import android.Manifest;
import android.content.ActivityNotFoundException;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.net.Uri;
import android.os.Bundle;
import android.support.annotation.NonNull;
import android.support.v4.app.ActivityCompat;
import android.support.v4.content.ContextCompat;
import android.support.v4.content.FileProvider;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.support.v7.widget.Toolbar;
import android.view.MenuInflater;
import android.view.MenuItem;
import android.view.View;
import android.webkit.MimeTypeMap;
import android.widget.PopupMenu;
import android.widget.TextView;

import java.io.File;
import java.util.Collections;
import java.util.List;

import aaron.filecommand.R;
import aaron.filecommand.activity.helper.Helper;
import aaron.filecommand.adapter.FilesAdapter;
import aaron.filecommand.model.Storage;

import static aaron.filecommand.activity.helper.Helper.fileExt;

/**
 * Created by Yunwen on 11/11/2017.
 */

public class InternalStorageActivity extends AppCompatActivity implements
        FilesAdapter.OnFileItemListener{
    private static final int PERMISSION_REQUEST_CODE = 1000;
    private RecyclerView mRecyclerView;
    private FilesAdapter mFilesAdapter;
    private Storage mStorage;
    private TextView mPathView;
    private TextView mMovingText;
    private boolean mCopy;
    private View mMovingLayout;
    private int mTreeSteps = 0;
    private final static String IVX = "abcdefghijklmnop";
    private final static String SECRET_KEY = "secret1234567890";
    private final static byte[] SALT = "0000111100001111".getBytes();
    private String mMovingPath;
    private boolean mInternal = false;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        // init content
        mStorage = new Storage(getApplicationContext());

        setContentView(R.layout.activity_internal_stoage);
        Toolbar toolbar = (Toolbar) findViewById(R.id.toolbar);
        setSupportActionBar(toolbar);

        mRecyclerView = (RecyclerView) findViewById(R.id.recycler);
        mPathView = (TextView) findViewById(R.id.path);


        RecyclerView.LayoutManager layoutManager = new LinearLayoutManager(this);
        mRecyclerView.setLayoutManager(layoutManager);
        mFilesAdapter = new FilesAdapter(getApplicationContext());
        mFilesAdapter.setListener(this);
        mRecyclerView.setAdapter(mFilesAdapter);
        mPathView.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                //showPathMenu();
            }
        });

        // load files
        showFiles(mStorage.getExternalStorageDirectory());

        checkPermission();
    }

    private void showFiles(String path) {
        mPathView.setText(path);
        List<File> files = mStorage.getFiles(path);
//        if (files != null) {
//            Collections.sort(files, OrderType.NAME.getComparator());
//        }
        mFilesAdapter.setFiles(files);
        mFilesAdapter.notifyDataSetChanged();
    }

    @Override
    public void onLongClick(File file) {
        //UpdateItemDialog.newInstance(file.getAbsolutePath()).show(getFragmentManager(), "update_item");
    }

    @Override
    public void onBackPressed() {
        if (mTreeSteps > 0) {
            String path = getPreviousPath();
            mTreeSteps--;
            showFiles(path);
            return;
        }
        super.onBackPressed();
    }

    private String getCurrentPath() {
        return mPathView.getText().toString();
    }

    private String getPreviousPath() {
        String path = getCurrentPath();
        int lastIndexOf = path.lastIndexOf(File.separator);
        if (lastIndexOf < 0) {
            Helper.showSnackbar("Can't go anymore", mRecyclerView);
            return getCurrentPath();
        }
        return path.substring(0, lastIndexOf);
    }
    @Override
    public void onClick(File file) {
        if (file.isDirectory()) {
            mTreeSteps++;
            String path = file.getAbsolutePath();
            showFiles(path);
        } else {

            try {
                Intent intent = new Intent(Intent.ACTION_VIEW);
                String mimeType =  MimeTypeMap.getSingleton().getMimeTypeFromExtension(fileExt(file.getAbsolutePath()));
                Uri apkURI = FileProvider.getUriForFile(
                        this,
                        getApplicationContext()
                                .getPackageName() + ".provider", file);
                intent.setDataAndType(apkURI, mimeType);
                intent.addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION);
                startActivity(intent);
            } catch (ActivityNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
    private void checkPermission() {
        if (ContextCompat.checkSelfPermission(this, Manifest.permission.WRITE_EXTERNAL_STORAGE) != PackageManager
                .PERMISSION_GRANTED) {
            ActivityCompat.requestPermissions(this,
                    new String[]{Manifest.permission.WRITE_EXTERNAL_STORAGE},
                    PERMISSION_REQUEST_CODE);
        }
    }

//    private void showPathMenu() {
//        PopupMenu popupmenu = new PopupMenu(this, mPathView);
//        MenuInflater inflater = popupmenu.getMenuInflater();
//        inflater.inflate(R.menu.path_menu, popupmenu.getMenu());
//
//        popupmenu.getMenu().findItem(R.id.go_internal).setVisible(!mInternal);
//        popupmenu.getMenu().findItem(R.id.go_external).setVisible(mInternal);
//
//        popupmenu.show();
//
//        popupmenu.setOnMenuItemClickListener(new PopupMenu.OnMenuItemClickListener() {
//
//            @Override
//            public boolean onMenuItemClick(MenuItem item) {
//                switch (item.getItemId()) {
//                    case R.id.go_up:
//                        String previousPath = getPreviousPath();
//                        mTreeSteps = 0;
//                        showFiles(previousPath);
//                        break;
//                    case R.id.go_internal:
//                        showFiles(mStorage.getInternalFilesDirectory());
//                        mInternal = true;
//                        break;
//                    case R.id.go_external:
//                        showFiles(mStorage.getExternalStorageDirectory());
//                        mInternal = false;
//                        break;
//                }
//                return true;
//            }
//        });
//    }

    @Override
    public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[]
            grantResults) {
        if (grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
            showFiles(mStorage.getExternalStorageDirectory());
        } else {
            finish();
        }
    }
}