package com.example.mattia.map0;

import android.app.Activity;
import android.content.Context;
import android.os.Bundle;
import android.webkit.WebView;


public class MainActivity extends Activity {

    WebView webView;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        webView = (WebView) findViewById(R.id.webview);

        /* Non necessario */
        //final JavaScriptInterface javaScriptInterface = new JavaScriptInterface(this);
        //webView.addJavascriptInterface(javaScriptInterface, "initialize");

        webView.getSettings().setJavaScriptEnabled(true);

        webView.loadUrl("file:///android_asset/map-simple.html");
    }

    /* Non necessario */
    public class JavaScriptInterface {
        Context context;

        public JavaScriptInterface(Context context) {
            this.context = context;
        }

        public void closeMyActivity() {
            finish();
        }
    }
}