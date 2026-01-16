package com.example.vichu.roomrecyclerlist;

import android.util.Log;

public class TraceLog {

    private static final String TAG = "[VISHNU] ";

    public static void entryLog() {
        StackTraceElement element = Thread.currentThread().getStackTrace()[3];
        String className = element.getClassName();
        className = className.substring(className.lastIndexOf(".") + 1, className.length());
        String methodName = element.getMethodName();
        Log.i(TAG + className, methodName + " >>> Entry");
    }

    public static void exitLog() {
        StackTraceElement element = Thread.currentThread().getStackTrace()[3];
        String className = element.getClassName();
        className = className.substring(className.lastIndexOf(".") + 1, className.length());
        String methodName = element.getMethodName();
        Log.i(TAG + className, methodName + " <<< Exit");
    }

    public static void log(String message) {
        StackTraceElement element = Thread.currentThread().getStackTrace()[3];
        String className = element.getClassName();
        className = className.substring(className.lastIndexOf(".") + 1, className.length());
        String methodName = element.getMethodName();
        Log.i(TAG + className, methodName + " : " + message);
    }

    public static void log(int logPriority, String message) {
        StackTraceElement element = Thread.currentThread().getStackTrace()[3];
        String className = element.getClassName();
        className = className.substring(className.lastIndexOf(".") + 1, className.length());
        String methodName = element.getMethodName();
        Log.println(logPriority, TAG + className + methodName, message);
    }

    public static void log(int logPriority, String tag, String message) {
        StackTraceElement element = Thread.currentThread().getStackTrace()[3];
        String className = element.getClassName();
        className = className.substring(className.lastIndexOf(".") + 1, className.length());
        String methodName = element.getMethodName();
        Log.println(logPriority, TAG + className + methodName + " : " + tag, message);
    }

    public static void exceptionLog(Exception e) {
        StackTraceElement element = Thread.currentThread().getStackTrace()[3];
        String className = element.getClassName();
        className = className.substring(className.lastIndexOf(".") + 1, className.length());
        String methodName = element.getMethodName();
        Log.println(Log.ASSERT, TAG + className, methodName + " Exception : " + e.getMessage());
    }


}