package com.example.jin.myapplication.webviewfiledemo;

import android.annotation.SuppressLint;
import android.app.Activity;
import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.util.Log;
import android.webkit.JsResult;
import android.webkit.WebChromeClient;
import android.webkit.WebView;
import android.widget.Toast;

import com.example.jin.myapplication.R;

@SuppressLint({ "SetJavaScriptEnabled", "JavascriptInterface" })
public class WebViewActivity extends Activity {
    private WebView webView;
    private Uri mUri;
    private String url;
    String mUrl1 = "file:///android_asset/html/attack_file.html";
//    String mUrl2 = "file:///android_asset/html/test.html";


//    @SuppressLint("JavascriptInterface")
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_web_view);
        webView = (WebView) findViewById(R.id.webview);
        webView.getSettings().setJavaScriptEnabled(true);
        webView.addJavascriptInterface(new JSInterface(), "jsInterface");
        webView.getSettings().setAllowFileAccessFromFileURLs(true);
        //webView.getSettings().setAllowFileAccess(false);
        webView.setWebChromeClient(new WebChromeClient() {
            @Override
            public boolean onJsAlert(WebView view, String url, String message,JsResult result) {
                //Required functionality here
                return super.onJsAlert(view, url, message, result);
            }
        });

        Intent i = getIntent();
        if (i != null) {
            mUri = i.getData();
        }
        if (mUri != null) {
            url = mUri.toString();
        }
        if (url != null) {
            webView.loadUrl(url);
        }

        webView.loadUrl(mUrl1);
    }



    class JSInterface {
        public String onButtonClick(String text) {
            final String str = text;
            runOnUiThread(new Runnable() {
                @Override
                public void run() {
                    Log.e("leehong2", "onButtonClick: text = " + str);
                    Toast.makeText(getApplicationContext(), "onButtonClick: text = " + str, Toast.LENGTH_LONG).show();
                }
            });

            return "This text is returned from Java layer.  js text = " + text;
        }

        public void onImageClick(String url, int width, int height) {
            final String str = "onImageClick: text = " + url + "  width = " + width + "  height = " + height;
            Log.i("leehong2", str);
            runOnUiThread(new Runnable() {
                @Override
                public void run() {
                    Toast.makeText(getApplicationContext(), str, Toast.LENGTH_LONG).show();
                }
            });
        }
    }

}