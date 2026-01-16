package com.blstreampatronage.patrykkrawczyk.activities;

import android.support.v7.app.ActionBar;
import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Build;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.support.v7.widget.GridLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.util.Log;
import android.view.View;
import android.view.WindowManager;
import android.widget.ProgressBar;
import com.blstreampatronage.patrykkrawczyk.ImageAdapter;
import com.blstreampatronage.patrykkrawczyk.JsonImageArray;
import com.blstreampatronage.patrykkrawczyk.events.LoadNewImagesEvent;
import com.blstreampatronage.patrykkrawczyk.R;
import com.blstreampatronage.patrykkrawczyk.events.RequestNewImagesEvent;
import com.blstreampatronage.patrykkrawczyk.SingleImage;
import com.blstreampatronage.patrykkrawczyk.connection.ConnectionHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nostra13.universalimageloader.core.ImageLoader;
import com.nostra13.universalimageloader.core.ImageLoaderConfiguration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import butterknife.Bind;
import butterknife.ButterKnife;
import de.greenrobot.event.EventBus;

/**
 * Activity responsible for displaying a set of images
 * Set "BASE_SERVER_URL" to your server that all calls should be directed to
 */
public class ImagesActivity extends AppCompatActivity {

    @Bind(R.id.progressBar)
    ProgressBar mProgressBar;
    @Bind(R.id.recyclerView)
    RecyclerView recyclerView;

    private ArrayList<SingleImage> mImageList;
    private final EventBus eventBus = EventBus.getDefault();
    private int mCurrentPage = -1;
    private boolean mContinueLoading = true;        // used to finish loading when there is no more pages to load
    private boolean mDoneLoadingCurrentPage = true; // used to not allow multiple downloading of same page

    private final static String BASE_SERVER_URL = "http://192.168.88.250:8080/"; // remember about trailing /

    /**
     * Method ran when the activity starts
     * Flow of method: initialize activity, initialize ButterKnife and EventBus
     * hide notification bar, go fullscreen,
     * initialize RecyclerView with GridLayout,
     * set listener to detect bottom of list
     * initialize Universal Image Loader and fetch first batch of images
     */
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_images);

        ButterKnife.bind(this);
        eventBus.register(this);

        // Hide action bar
        ActionBar ab = null;
        try {
            ab = getSupportActionBar();
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
        if (null != ab) ab.hide();

        // Hide notification/status bar
        if (Build.VERSION.SDK_INT < 16) {
            int flagFullscreen = WindowManager.LayoutParams.FLAG_FULLSCREEN;
            getWindow().setFlags(flagFullscreen, flagFullscreen);
        } else {
            View decorView = getWindow().getDecorView();
            decorView.setSystemUiVisibility(View.SYSTEM_UI_FLAG_FULLSCREEN);
        }

        mImageList = new ArrayList<>();

        //recyclerView.setHasFixedSize(true);

        RecyclerView.LayoutManager layoutManager;
        if (getResources().getBoolean(R.bool.isTablet)) {
            layoutManager = new GridLayoutManager(this, 4);
        } else {
            layoutManager = new GridLayoutManager(this, 2);
        }

        recyclerView.setLayoutManager(layoutManager);

        ImageAdapter myAdapter = new ImageAdapter(this, mImageList);
        recyclerView.setAdapter(myAdapter);

        // detecting bottom of list
        RecyclerView.OnScrollListener onScrollListener =  new RecyclerView.OnScrollListener() {
            @Override
            public void onScrolled(RecyclerView recyclerView, int dx, int dy) {
                GridLayoutManager layoutManager = (GridLayoutManager)recyclerView.getLayoutManager();

                if (layoutManager.findLastVisibleItemPosition() == mImageList.size() - 1) {
                    bottomDetected();
                }

            }
        };
        recyclerView.addOnScrollListener(onScrollListener);

        //initialize UIL
        ImageLoaderConfiguration config = new ImageLoaderConfiguration.Builder(this)
                .build();
        ImageLoader.getInstance().init(config);

        // initialize list
        bottomDetected();
        Log.d("TAG", "end of onCreate");
    }

    /**
     * Part of EventBus, DownloadTask posts this Event to signal that new images are ready to be loaded
     * If isError then there's no more images to be loaded
     * If there's no error, feed new images to loader
     * @param event instance containing message string and error boolean
     */
    public void onEvent(LoadNewImagesEvent event) {
        Log.d("TAG", "LoadNewImagesEvent");
        if (event.isError == false) {
            mContinueLoading = true;
            feedImages(event.message);
        } else {
            mContinueLoading = false;

            runOnUiThread(new Runnable() {
                @Override
                public void run() {
                    mProgressBar.setVisibility(View.INVISIBLE);
                }
            });
        }
    }
    /**
     * Part of EventBus, getPage() posts this Event to signal that new images should be downloaded
     * Due to ConnectionHandler, the requested GET request will be ran in AsyncTask
     * @param event instance containing message string that's a string to URL to be downloaded
     */
    public void onEvent(RequestNewImagesEvent event) {
        Log.d("TAG", "RequestNewImagesEvent");
        ConnectionHandler.getInstance().doGetRequest(event.message);
    }

    /**
     * Method ran when bottom is reached, invokes getPage() if current page is already loaded
     */
    private void bottomDetected() {
        Log.d("TAG", "bottomDetected");
        if (mDoneLoadingCurrentPage) {
            mProgressBar.setVisibility(View.VISIBLE);
            ++mCurrentPage;
            mDoneLoadingCurrentPage = false;
            getPage();
        }
    }

    /**
     * Build URL to new page that needs to be loaded
     * Post new Event to signal the URL that needs to be downloaded
     */
    private void getPage() {
        Log.d("TAG", "getPage");
        String url = BASE_SERVER_URL + "page_" + mCurrentPage + ".json";

        mContinueLoading = false;

        EventBus.getDefault().post(new RequestNewImagesEvent(url));
    }

    /**
     * Map provided String with Jackson, traverse map and add new instances of SingleImage
     * to array, this array is used by the adapter to show images
     * @param response String containing unformatted JSON, will be fed to Jackson mapper
     */
    private void feedImages(String response) {
        Log.d("TAG", "feedImages");
        JsonImageArray imageArray = null;

        ObjectMapper mapper = new ObjectMapper(); // Jackson magic
        try {
            imageArray = mapper.readValue(response, JsonImageArray.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (null != imageArray) {
            Collection<SingleImage> array = imageArray.getArray();
            for (SingleImage image : array)
                mImageList.add(image);
        }

        mDoneLoadingCurrentPage = true;
    }

    /**
     * Logout user by defaulting values in SharedPreferences file
     * @param v Button that was clicked to log out the user
     */
    public void onLogOutUser(View v) {
        SharedPreferences sharedPref = getSharedPreferences("UserInformation", MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPref.edit();

        editor.putString(getString(R.string.userEmailKey), null);
        editor.putString(getString(R.string.userPasswordKey), null);
        editor.apply();

        Intent intent = new Intent(this, LoginActivity.class);
        startActivity(intent);
        finish();
    }

    /**
     * When app closes, make sure AsyncTasks are canceled from ConnectionHandler
     */
    @Override
    protected void onStop() {
        super.onStop();

        boolean completion = false;
        completion = ConnectionHandler.getInstance().finishAllTasks();
        if (completion == false)
            Log.d("TAG", "Error during cleaning of async tasks in ConnectionHandler.finishAllTasks()");
    }
}
