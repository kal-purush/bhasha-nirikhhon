package com.erobbing.screenrecorder;

import android.app.Activity;
import android.content.Context;
import android.content.Intent;
import android.content.res.Configuration;
import android.os.Bundle;
import android.os.Environment;
import android.os.Handler;
import android.os.ServiceManager;
import android.os.storage.IMountService;
import android.os.storage.StorageManager;
import android.os.storage.StorageVolume;
import android.view.KeyEvent;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.Button;
import android.widget.CompoundButton;
import android.widget.Switch;
import android.widget.TextView;
import android.widget.Toast;

import com.erobbing.screenrecorder.R.id;
import com.erobbing.view.DropEditText;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ArrayList;
import java.util.List;

import android.util.Log;

import com.erobbing.squareprogressbar.SquareProgressBar;
import com.erobbing.squareprogressbar.utils.PercentStyle;
import com.erobbing.util.ArithmeticUtil;

import android.graphics.Paint.Align;

import java.math.BigDecimal;
import java.lang.reflect.Method;
import java.io.File;

/**
 * Created by zhangzhaolei on 2016/11/24.
 */

public class ScreenRecorderActivity extends Activity {

    private static final String TAG = "ScreenRecording";
    private String mRecordingTime = "";//180 seconds
    private String mRecordingSize = "";//default screen size
    private String mRecordingBitrate = "";//default 25000000
    private static final String SPACE = " ";
    private String mSavePath;

    private DropEditText dropTime;
    private DropEditText dropSize;
    private DropEditText dropBitrate;
    private Switch savePath;

    public SquareProgressBar squareProgressBar;

    private int mSeconds;
    private int pregress = 0;
    private double rate;
    private Handler mStopHandler = new Handler();
    private Runnable mStopRunnable = new Runnable() {
        @Override
        public void run() {
            mSeconds--;
            pregress++;
            if (mSeconds <= 0) {
                Log.d(TAG, "ScreenRecording complete");
                squareProgressBar.setProgress(100);
                pregress = 0;
                mStopHandler.removeCallbacks(mStopRunnable);
                Toast.makeText(ScreenRecorderActivity.this, R.string.toast_title, Toast.LENGTH_SHORT).show();
                finish();
            } else {
                squareProgressBar.setProgress(pregress * rate);
                squareProgressBar.setEnabled(false);
                mStopHandler.postDelayed(mStopRunnable, 1000);
            }
        }
    };

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
    }

    @Override
    protected void onResume() {
        super.onResume();
        init();
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
    }

    @Override
    public void onConfigurationChanged(Configuration newConfig) {
        super.onConfigurationChanged(newConfig);
        if (this.getResources().getConfiguration().orientation == Configuration.ORIENTATION_LANDSCAPE) {
            setContentView(R.layout.activity_main);
            init();
        }
        if (this.getResources().getConfiguration().orientation == Configuration.ORIENTATION_PORTRAIT) {
            setContentView(R.layout.activity_main);
            init();
        }
    }

    public void init() {
        String[] time = getResources().getStringArray(R.array.time);
        String[] size = getResources().getStringArray(R.array.size);
        String[] bitrate = getResources().getStringArray(R.array.bitrate);

        final List<String> timeList = new ArrayList<String>();
        for (int i = 0; i < time.length; i++) {
            timeList.add(time[i]);
        }

        final List<String> sizeList = new ArrayList<String>();
        for (int i = 0; i < size.length; i++) {
            sizeList.add(size[i]);
        }

        final List<String> bitrateList = new ArrayList<String>();
        for (int i = 0; i < bitrate.length; i++) {
            bitrateList.add(bitrate[i]);
        }
        dropTime = (DropEditText) findViewById(id.drop_edit_time);
        dropSize = (DropEditText) findViewById(id.drop_edit_size);
        dropBitrate = (DropEditText) findViewById(id.drop_edit_bitrate);
        savePath = (Switch) findViewById(id.switch_save_path);

        squareProgressBar = (SquareProgressBar) findViewById(R.id.progress);
        squareProgressBar.setImage(R.drawable.btn_bg);
        squareProgressBar.setColor(getResources().getString(R.color.squareprogress_color));
        squareProgressBar.setProgress(0);
        squareProgressBar.setWidth(getResources().getInteger(R.integer.config_squareprogress_width));
        squareProgressBar.showProgress(true);
        squareProgressBar.setPercentStyle(new PercentStyle(Align.CENTER, getResources().getDimension(R.dimen.squareProgress_textsize), true));

        dropTime.setAdapter(new BaseAdapter() {

            @Override
            public int getCount() {
                return timeList.size();
            }

            @Override
            public Object getItem(int position) {
                return timeList.get(position);
            }

            @Override
            public long getItemId(int position) {
                return position;
            }

            @Override
            public View getView(int position, View convertView, ViewGroup parent) {
                TextView tv = new TextView(ScreenRecorderActivity.this);
                tv.setTextSize(getResources().getDimension(R.dimen.dropdown_list_textsize));
                tv.setText(timeList.get(position));
                return tv;
            }
        });

        dropSize.setAdapter(new BaseAdapter() {

            @Override
            public int getCount() {
                return sizeList.size();
            }

            @Override
            public Object getItem(int position) {
                return sizeList.get(position);
            }

            @Override
            public long getItemId(int position) {
                return position;
            }

            @Override
            public View getView(int position, View convertView, ViewGroup parent) {
                TextView tv = new TextView(ScreenRecorderActivity.this);
                tv.setTextSize(getResources().getDimension(R.dimen.dropdown_list_textsize));
                tv.setText(sizeList.get(position));
                return tv;
            }
        });

        dropBitrate.setAdapter(new BaseAdapter() {

            @Override
            public int getCount() {
                return bitrateList.size();
            }

            @Override
            public Object getItem(int position) {
                return bitrateList.get(position);
            }

            @Override
            public long getItemId(int position) {
                return position;
            }

            @Override
            public View getView(int position, View convertView, ViewGroup parent) {
                TextView tv = new TextView(ScreenRecorderActivity.this);
                tv.setTextSize(getResources().getDimension(R.dimen.dropdown_list_textsize));
                tv.setText(bitrateList.get(position));
                return tv;
            }
        });

        if (getExternalSDStorageState()) {
            savePath.setEnabled(true);
        } else {
            savePath.setEnabled(false);
        }
        mSavePath = getInternalSDPath() + "/";
        savePath.setOnCheckedChangeListener(new CompoundButton.OnCheckedChangeListener() {
            @Override
            public void onCheckedChanged(CompoundButton buttonView, boolean isChecked) {
                mSavePath = isChecked ? (getExternalSDPath(ScreenRecorderActivity.this) + "/") : getInternalSDPath();
                Log.d(TAG, "getExternalSDStorageState()=" + getExternalSDStorageState());
            }
        });

        squareProgressBar.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                StartRecording(dropTime.getText().toString(), dropSize.getText().toString(), dropBitrate.getText().toString(), mSavePath);
                boolean isFullTime = "".equals(dropTime.getText().toString());
                mSeconds = isFullTime ? 180 : Integer.parseInt(dropTime.getText().toString());
                rate = ArithmeticUtil.div((double) 100, (double) mSeconds, 2);
                mStopHandler.postDelayed(mStopRunnable, 1000);
                squareProgressBar.setProgress(0);
                squareProgressBar.setEnabled(false);
                Log.d(TAG, "Recording time=" + mSeconds + "savePath=" + mSavePath);
            }
        });
    }

    public void StartRecording(String time, String size, String bitrate, String path) {
        String cmd_Time = "".equals(time) ? "" : " --time-limit " + time;
        String cmd_Size = "".equals(size) ? "" : " --size " + size;
        String cmd_bitrate = "".equals(bitrate) ? "" : " --bit-rate " + bitrate;
        try {
            Runtime.getRuntime().exec("screenrecord" + cmd_Time + cmd_Size + cmd_bitrate + SPACE + path + getCurrentTime() + ".mp4");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getCurrentTime() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy.MMdd.HH.mm.ss");
        String currentTime = formatter.format(new Date(System.currentTimeMillis()));
        return currentTime;
    }

    @Override
    public boolean onKeyDown(int keyCode, KeyEvent event) {
        if (keyCode == KeyEvent.KEYCODE_BACK) {
            moveTaskToBack(false);
            return true;
        }
        return super.onKeyDown(keyCode, event);
    }

    public boolean getExternalSDStorageState() {
        try {
            IMountService mMountService = IMountService.Stub.asInterface(ServiceManager.getService("mount"));
            Log.d(TAG, "sdcard volume state " + mMountService.getVolumeState(getExternalSDPath(this)));
            return "mounted".equals(mMountService.getVolumeState(getExternalSDPath(this)));
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static String getExternalSDPath(Context context) {
        String sd = null;
        StorageManager mStorageManager = (StorageManager) context
                .getSystemService(Context.STORAGE_SERVICE);
        StorageVolume[] volumes = mStorageManager.getVolumeList();
        for (int i = 0; i < volumes.length; i++) {
            if (volumes[i].isRemovable() && volumes[i].allowMassStorage()
                    && volumes[i].getDescription(context).contains("SD")) {
                sd = volumes[i].getPath();
            }
        }
        return sd;
    }

    /**
     * get internal storage path
     *
     * @return
     */
    public static String getInternalSDPath() {
        return Environment.getExternalStorageDirectory().getPath();
    }

    /**
     * reflect get internal storage path
     *
     * @return
     */
    public static String getInternalStoragePath() {
        String ret = null;

        try {
            Method method = Environment.class.getMethod("getExternalStorageDirectory",
                    (Class[]) null);
            File file = (File) method.invoke(null, (Object[]) null);
            ret = file.getPath();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }
}