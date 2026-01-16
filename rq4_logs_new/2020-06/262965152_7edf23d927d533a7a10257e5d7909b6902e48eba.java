package com.example.upgradegame;

import android.app.Application;
import android.util.Log;

import com.bun.miitmdid.core.IIdentifierListener;
import com.bun.miitmdid.core.JLibrary;
import com.bun.miitmdid.core.MdidSdkHelper;
import com.bun.miitmdid.supplier.IdSupplier;
import com.example.upgradegame.tencent.TConstant;
import com.kuaiyou.open.InitSDKManager;
import com.qq.e.comm.managers.GDTADManager;
import com.qq.e.comm.managers.setting.GlobalSetting;

import io.flutter.app.FlutterApplication;

public class App extends FlutterApplication {
    @Override
    public void onCreate() {
        super.onCreate();
        // adView 初始化
        InitSDKManager.getInstance().init(getApplicationContext());
        //集成信通院MSA http://msa-alliance.cn/
        try {
            // 初始化MSA SDK
            JLibrary.InitEntry(this);
            //获取OAID
            int sdkState = MdidSdkHelper.InitSdk(getApplicationContext(), true, new IIdentifierListener() {
                @Override
                public void OnSupport(boolean b, IdSupplier idSupplier) {
                    if (idSupplier != null) {
                        String oaid = idSupplier.getOAID();
                        Log.e("oaid","oaid=" + oaid);
                    }
                }
            });
            Log.e("mdidsdk","初始化" + sdkState);
        } catch (Throwable tr) {
            tr.printStackTrace();
        }
        // 腾讯广告 通过调用此方法初始化 SDK。如果需要在多个进程拉取广告，每个进程都需要初始化 SDK。
        GDTADManager.getInstance().initWith(this, TConstant.APPID);

        GlobalSetting.setChannel(1);
        GlobalSetting.setEnableMediationTool(true);
    }
}