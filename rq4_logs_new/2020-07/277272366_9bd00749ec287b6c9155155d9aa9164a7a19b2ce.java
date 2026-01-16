package com.rulerbug.bugutils.Utils;

import android.app.ActivityManager;
import android.content.ComponentName;
import android.content.Context;

import java.util.List;

class BugAppIsBackgroundUtils {
    public static boolean isApplicationBroughtToBackground() {
        ActivityManager am = (ActivityManager) BugApp.getContext().getSystemService(Context.ACTIVITY_SERVICE);
        List<ActivityManager.RunningTaskInfo> tasks = am.getRunningTasks(1);
        if (!tasks.isEmpty()) {
            ComponentName topActivity = tasks.get(0).topActivity;
            if (!topActivity.getPackageName().equals(BugApp.getContext().getPackageName())) {
                return true;
            }
        }
        return false;
    }

}