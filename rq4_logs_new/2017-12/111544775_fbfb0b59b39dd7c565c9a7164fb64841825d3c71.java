package com.whatsmode.shopify.block;

import android.content.Context;
import android.webkit.JavascriptInterface;

import com.whatsmode.shopify.common.Constant;
import com.whatsmode.shopify.ui.helper.SDFileHelper;
import com.zchu.log.Logger;

import java.io.File;

import okhttp3.OkHttpClient;

/**
 * Created by Administrator on 2017/12/19.
 */

public class JsInterfaceObtainImage {
    private final long currentTimeStep;
    private Context context;

    public JsInterfaceObtainImage(Context context,long currentTimeStep) {
        this.currentTimeStep = currentTimeStep;
        this.context = context;
    }

    @JavascriptInterface
    public void getImage(String[] urls) {

        Logger.e("========进入js方法========" + urls[0]);
        if (urls != null && urls.length > 0) {
//            for (int i = 0; i < urls.length; i++) {
//                SDFileHelper.savaFileToSD(urls);
//                Logger.e("===" + i + "==="+urls[i]);
//            }
            SDFileHelper.savePicture(currentTimeStep+"",urls[0]);
        }
    }
}