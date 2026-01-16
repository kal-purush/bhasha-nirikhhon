package com.xsylsb.integrity.util;

import android.app.ActivityManager;
import android.content.ClipData;
import android.content.ClipboardManager;
import android.content.Context;
import android.content.SharedPreferences;
import android.content.pm.PackageInfo;
import android.content.pm.PackageManager;
import android.os.Build;
import android.support.annotation.RequiresApi;


import java.util.List;

public class BaseUtils {

    public static final String KEY_USER_ID = "userId";

    public static final String KEY_WX_OPEN_ID = "WXOpenId";

    public static final String KEY_PROJECT_ID = "projectId";

    public static final String KEY_PROJECT_DATA = "projectData";

    public static final String KEY_PROJECT_TIME = "projectTime";

    public static final String KEY_PROJECT_STEP = "projectStep";

    /**
     * 获取版本号
     *
     * @param context
     * @return
     */
    public static int getVersionCode(Context context) {
        PackageManager pm = context.getPackageManager();
        int code = 0;
        try {
            PackageInfo packageInfo = pm.getPackageInfo(context.getPackageName(), 0);
            code = packageInfo.versionCode;
        } catch (PackageManager.NameNotFoundException e) {
            e.printStackTrace();
        }
        return code;
    }

    /**
     * 获取版本名称
     *
     * @param context
     * @return
     */
    public static String getVersionName(Context context) {
        PackageManager manager = context.getPackageManager();
        String name = "";
        try {
            PackageInfo info = manager.getPackageInfo(context.getPackageName(), 0);
            name = info.versionName;
        } catch (PackageManager.NameNotFoundException e) {
            e.printStackTrace();
        }
        return name;
    }

    /**
     * 保存值
     *
     * @param context
     * @param key
     * @param value
     */
    public static void saveValue(Context context, String key, String value) {
        SharedPreferences sharedPreferences = context.getApplicationContext()
                .getSharedPreferences("base", Context.MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPreferences.edit();
        editor.putString(key, value);
        editor.apply();
    }

    /**
     * 获取值
     *
     * @param context
     * @param key
     * @return
     */
    public static String getValue(Context context, String key) {
        SharedPreferences sharedPreferences = context.getApplicationContext()
                .getSharedPreferences("base", Context.MODE_PRIVATE);
        return sharedPreferences.getString(key, "");
    }



    /**
     * 检查版本
     *
     * @param context
     * @param version
     * @return
     */
    public static boolean checkVersion(Context context, String version) {
        String[] versions = version.split("\\.");
        String[] curVersions = BaseUtils.getVersionName(context).split("\\.");
        boolean hasNewVersion = false;
        if (DataFormatUtil.stringToInt(versions[0]) > DataFormatUtil.stringToInt(curVersions[0])) {
            hasNewVersion = true;
        } else if (DataFormatUtil.stringToInt(versions[1]) > DataFormatUtil.stringToInt(curVersions[1])) {
            hasNewVersion = true;
        } else if (DataFormatUtil.stringToInt(versions[2]) > DataFormatUtil.stringToInt(curVersions[2])) {
            hasNewVersion = true;
        }
        return hasNewVersion;
    }

}