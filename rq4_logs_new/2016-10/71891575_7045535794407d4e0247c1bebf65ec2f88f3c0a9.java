package gdky005.accesibilitydemo;

import android.accessibilityservice.AccessibilityService;
import android.annotation.SuppressLint;
import android.content.Intent;
import android.os.Handler;
import android.os.Message;
import android.util.Log;
import android.view.accessibility.AccessibilityEvent;
import android.view.accessibility.AccessibilityNodeInfo;
import android.widget.Button;
import android.widget.TextView;

import java.util.List;

/**
 * Created by WangQing on 2016/10/25.
 */

public class WQAccessibilityServiceBack extends AccessibilityService {

    private static final String TAG = "WQAccessibilityService";
    private static final String TEXTVIEW = TextView.class.getCanonicalName();
    private static final String BUTTON = Button.class.getCanonicalName();

    Handler handler = new Handler() {

        @Override
        public void handleMessage(Message msg) {
            super.handleMessage(msg);

            if (msg.what == 0) {
                Log.i(TAG, "onAccessibilityEvent:  找到搜索按钮了，而且我要点击下");

                AccessibilityNodeInfo node = (AccessibilityNodeInfo) msg.obj;
                node.performAction(AccessibilityNodeInfo.ACTION_CLICK);
            }

        }
    };



    /**
     * 系统会在成功连接上你的服务的时候调用这个方法，在这个方法里你可以做一下初始化工作，例如设备的声音震动管理，也可以调用setServiceInfo()进行配置工作。
     */
    @Override
    protected void onServiceConnected() {
//        super.onServiceConnected();
        //可以在这里初始化对应的信息


        //第一种：我们在代码中注册多个应用的包名，从而可以监听多个应用:
//        AccessibilityServiceInfo info = getServiceInfo();
//        //这里可以设置多个包名，监听多个应用
//        info.packageNames = new String[]{"xxx.xxx.xxx", "yyy.yyy.yyy","...."};
//        setServiceInfo(info);


//可用代码配置当前Service的信息
        //		AccessibilityServiceInfo info = new AccessibilityServiceInfo();
        //		info.packageNames = installPackge; //监听过滤的包名
        //		info.eventTypes = AccessibilityEvent.TYPES_ALL_MASK; //监听哪些行为
        //		info.feedbackType = AccessibilityServiceInfo.FEEDBACK_SPOKEN; //反馈
        //		info.notificationTimeout = 100; //通知的时间
        //		setServiceInfo(info);



//        AccessibilityServiceInfo info = getServiceInfo();
//        info.eventTypes = AccessibilityEvent.TYPES_ALL_MASK;
//        info.feedbackType = AccessibilityServiceInfo.FEEDBACK_SPOKEN;
//        info.notificationTimeout = 100;
//        setServiceInfo(info);
//        info.packageNames = new String[]{"xxx.xxx.xxx", "yyy.yyy.yyy","...."};
//        setServiceInfo(info);
        super.onServiceConnected();
    }

    /**
     * 通过这个函数可以接收系统发送来的AccessibilityEvent，接收来的AccessibilityEvent是经过过滤的，过滤是在配置工作时设置的。
     *
     * 这是异步通知
     *
     * @param event
     */
    @Override
    public void onAccessibilityEvent(AccessibilityEvent event) {

//        Log.i(TAG, "onAccessibilityEvent: " + event.getPackageName());
        nodeInfo(event);

















//        第二种：我们在onAccessibilityEvent事件监听的方法中做包名的过滤(这种方式最常用)
//        String pkgName = event.getPackageName().toString();
//        if("xxx.xxx.xxx".equals(pkgName)){
//
//        }else if("yyy.yyy.yyy".equals(pkgName)){
//
//        }else if("....".equals(pkgName)){
//
//        }

    }

    private void nodeInfo(AccessibilityEvent event) {
        AccessibilityNodeInfo nodeInfo = event.getSource();
        if (nodeInfo != null) {
            if (getRootInActiveWindow() == null)
                return;
            checkName(TEXTVIEW, "搜索");


//            //通过文字找到当前的节点
//            List<AccessibilityNodeInfo> nodes = getRootInActiveWindow().findAccessibilityNodeInfosByText("搜索");
//            for (int i = 0; i < nodes.size(); i++) {
//                AccessibilityNodeInfo node = nodes.get(i);
//                // 执行按钮点击行为
//                if (node.getClassName().equals("android.widget.TextView") && node.isEnabled()) {
//                    Log.i(TAG, "onAccessibilityEvent:  找到搜索按钮了，而且我要点击下");
//
//                    node.performAction(AccessibilityNodeInfo.ACTION_CLICK);
//                }
//            }
        }
    }

    private void checkName(String type, String keyWorld) {
        //通过文字找到当前的节点
        List<AccessibilityNodeInfo> nodes = getRootInActiveWindow().findAccessibilityNodeInfosByText(keyWorld);
        for (int i = 0; i < nodes.size(); i++) {
            AccessibilityNodeInfo node = nodes.get(i);
            if (node.getClassName().equals(type) && node.isEnabled()) {

                handler.removeMessages(0);

                Message msg = handler.obtainMessage();
                msg.what = 0;
                msg.obj = node;

                handler.sendMessageDelayed(msg, 3000);

//                node.performAction(AccessibilityNodeInfo.ACTION_CLICK);

//
            }
        }
    }

    /**
     * 这个在系统想要中断AccessibilityService返给的响应时会调用。在整个生命周期里会被调用多次。
     */
    @Override
    public void onInterrupt() {

    }


    @SuppressLint("NewApi")
    private void findAndPerformAction(String text) {
        // 查找当前窗口中包含“安装”文字的按钮
        if (getRootInActiveWindow() == null)
            return;
        //通过文字找到当前的节点
        List<AccessibilityNodeInfo> nodes = getRootInActiveWindow().findAccessibilityNodeInfosByText(text);
        for (int i = 0; i < nodes.size(); i++) {
            AccessibilityNodeInfo node = nodes.get(i);
            // 执行按钮点击行为
            if (node.getClassName().equals("android.widget.Button") && node.isEnabled()) {
                node.performAction(AccessibilityNodeInfo.ACTION_CLICK);
            }
        }
    }


    /**
     * 在系统将要关闭这个AccessibilityService会被调用。在这个方法中进行一些释放资源的工作。
     * @param intent
     * @return
     */
    @Override
    public boolean onUnbind(Intent intent) {
        return super.onUnbind(intent);
    }
}