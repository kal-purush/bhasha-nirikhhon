package com.xinlingfamen.app;

import com.umeng.analytics.MobclickAgent;
import com.xinlingfamen.app.core.home.HomeTabActivity;
import com.xinlingfamen.app.utils.SharePrefenceUtils;

import android.app.AlarmManager;
import android.app.PendingIntent;
import android.content.Context;
import android.content.Intent;
import android.os.Looper;
import android.widget.Toast;

/**
 * &lt;一句话功能简述&gt; &lt;功能详细描述&gt;
 *
 * @author xuff
 * @version [版本号, 2015/11/19]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class CrashHandler implements Thread.UncaughtExceptionHandler
{
    
    private Thread.UncaughtExceptionHandler mDefaultHandler;
    
    private static CrashHandler reportCrashBug = null;
    

    private XlfmApplication context;
    
    public static CrashHandler getInstance()
    {
        if (reportCrashBug == null)
            reportCrashBug = new CrashHandler();
        return reportCrashBug;
    }
    
    @Override
    public void uncaughtException(Thread thread, final Throwable throwable)
    {
        if (!handleException(throwable) && mDefaultHandler != null)
        {
            mDefaultHandler.uncaughtException(thread, throwable);
        }
        else
        {
            initSharePrefsHelp();
            int reStartTime = preferencesUtil.getIntPreference(APP_RESTART_TIME);
            if (reStartTime < 2)
            {
                int temp = reStartTime + 1;
                preferencesUtil.setIntPreference(APP_RESTART_TIME, reStartTime + 1);
                AlarmManager mgr = (AlarmManager)context.getSystemService(Context.ALARM_SERVICE);
                MobclickAgent.reportError(context, throwable);
                // token为空，需要跳转至登陆.
                Intent intent = new Intent();

                    intent.setClass(context, HomeTabActivity.class);

                intent.setFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP);
                intent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
                
                PendingIntent restartIntent =
                    PendingIntent.getActivity(context, 0, intent, PendingIntent.FLAG_ONE_SHOT);
                mgr.set(AlarmManager.RTC, System.currentTimeMillis() + 1000, restartIntent); // 1秒钟后重启应用
            }
            else
            {
                preferencesUtil.setIntPreference(APP_RESTART_TIME, 0);
            }
            
            android.os.Process.killProcess(android.os.Process.myPid());
            System.exit(10);
            System.gc();
            
        }
        
    }
    

    private SharePrefenceUtils preferencesUtil;
    private static final String APP_RESTART_TIME="APP_RESTART_TIME";
    private void initSharePrefsHelp()
    {
        if (preferencesUtil == null)
        {

            preferencesUtil =   SharePrefenceUtils.getInstance(context);
        }
    }
    
    private boolean handleException(Throwable ex)
    {
        if (ex == null)
        {
            return true;
        }
        // final String msg = ex.getLocalizedMessage();
        // 使用Toast来显示异常信息
        new Thread()
        {
            @Override
            public void run()
            {
                // Toast 显示需要出现在一个线程的消息队列中
                Looper.prepare();

                initSharePrefsHelp();
                int reStartTime = preferencesUtil.getIntPreference(APP_RESTART_TIME);
                if (reStartTime < 2)
                {
                    Toast.makeText(context.getApplicationContext(), "系统发生了不可预知的错误，重启中...",Toast.LENGTH_LONG).show();
                }
                else
                {
                    Toast.makeText(context.getApplicationContext(), "退出中...",Toast.LENGTH_LONG).show();

                }
                Looper.loop();
            }
        }.start();
        return true;
    }

    public void init(XlfmApplication context)
    {
        this.context = context;
        mDefaultHandler = Thread.getDefaultUncaughtExceptionHandler();
        Thread.setDefaultUncaughtExceptionHandler(this);
        preferencesUtil = SharePrefenceUtils.getInstance(context);

    }

}