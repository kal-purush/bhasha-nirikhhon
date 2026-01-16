package yunstudio2015.android.yunmeet.app;

import android.app.Application;
import android.content.Context;
import android.os.Build;

import com.android.volley.RequestQueue;
import com.android.volley.toolbox.Volley;
import com.nostra13.universalimageloader.cache.disc.impl.UnlimitedDiskCache;
import com.nostra13.universalimageloader.cache.disc.naming.Md5FileNameGenerator;
import com.nostra13.universalimageloader.core.ImageLoader;
import com.nostra13.universalimageloader.core.ImageLoaderConfiguration;
import com.nostra13.universalimageloader.core.assist.QueueProcessingType;
import com.nostra13.universalimageloader.utils.StorageUtils;

import java.io.File;

/**
 * Created by Ultima on 2015/10/27.
 */
public class MyApplication extends Application {

    public MyApplication mInstance;

    // app constants
    public static final boolean debug = true;
    public static String appname = "YunMeet";

    // teach something to others doesn't take it from you.
    // but not practising what you already know, clearly
    // is a way for you to get far and far from what
    // you are already have as knowledge.
    public static RequestQueue requestQueue;

    public MyApplication() {
    }

    @Override
    public void onCreate() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.GINGERBREAD) {
//            StrictMode.setThreadPolicy(new StrictMode.ThreadPolicy.Builder().detectAll().penaltyDialog().build());
//            StrictMode.setVmPolicy(new StrictMode.VmPolicy.Builder().detectAll().penaltyDeath().build());
        }
        super.onCreate();
        // create a new requestQueue.
        requestQueue = Volley.newRequestQueue(this);
        mInstance = this;
        // init image loader.
        initUIL (getApplicationContext());
    }


    public MyApplication getInstance () {
        return mInstance;
    }


    private void initUIL(Context context) {

        File cacheDir = StorageUtils.getCacheDirectory(context);
        ImageLoaderConfiguration  config = new ImageLoaderConfiguration.Builder(context)
//                .memoryCacheExtraOptions(480, 800) // default = device screen dimensions
                .memoryCacheExtraOptions(1080, 1920) // default = device screen dimensions
                .threadPriority(Thread.NORM_PRIORITY - 2)
                .denyCacheImageMultipleSizesInMemory()
                .diskCacheFileNameGenerator(new Md5FileNameGenerator())
                .diskCacheSize(50 * 1024 * 1024)  // 50 MiB
                .tasksProcessingOrder(QueueProcessingType.LIFO)
                .writeDebugLogs()  // Remove for release app
                .diskCache(new UnlimitedDiskCache(cacheDir))
                .build();
        // init the config
        ImageLoader.getInstance().init(config);
    }

    @Override
    public void onLowMemory() {
        super.onLowMemory();
    }
}