package jp.obcn.memoleep.pref;

import android.content.Context;
import android.content.SharedPreferences;

/**
 * Created by iwsbrfts on 2015/03/04.
 */
public class GlobalConfig {
    private static final String PREF_NAME = "config";
    private SharedPreferences mPref  =null;

    private static final Object S_LOCK = new Object();

    private static GlobalConfig sConfig;

    public static final GlobalConfig getInstance(Context context) {
        synchronized (S_LOCK) {
            sConfig = new GlobalConfig(context);
        }
        return sConfig;
    }

    private GlobalConfig(Context context){
        mPref = context.getSharedPreferences(PREF_NAME,Context.MODE_PRIVATE);
    }

    private static final String KEY_COMPLETED_LESSON = "completed_lesson_id_%s";

    public void writeCompletedLesson(String lessonId) {
        String key = String.format(KEY_COMPLETED_LESSON,lessonId);
        mPref.edit().putBoolean(key,true).apply();
    }


    public boolean readCompletedLesson(String lessonId) {
        String key = String.format(KEY_COMPLETED_LESSON,lessonId);
        return mPref.getBoolean(key,false);
    }


}