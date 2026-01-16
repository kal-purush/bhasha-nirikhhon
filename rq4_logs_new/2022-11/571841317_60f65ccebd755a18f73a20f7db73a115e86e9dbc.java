package jp.bizen.laggycameraapp;

import android.Manifest;
import android.annotation.SuppressLint;
import android.app.Activity;
import android.content.pm.PackageInfo;
import android.content.pm.PackageManager;
import android.os.Bundle;
import android.webkit.ConsoleMessage;
import android.webkit.PermissionRequest;
import android.webkit.WebChromeClient;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.widget.TextView;
import android.widget.Toast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntPredicate;

public class TopActivity extends Activity {
    private static final int REQUEST_PERMISSION_CODE = 10000;
    private static final String URL = "https://practice-75a12.web.app/";
    private static final String WEBVIEW_PACKAGE_ID = "com.google.android.webview";

    private final WebChromeClient webChromeClient = new WebChromeClient() {
        @Override
        public void onPermissionRequest(PermissionRequest request) {
            request.grant(request.getResources());
        }

        @Override
        public boolean onConsoleMessage(ConsoleMessage consoleMessage) {
            System.out.println(consoleMessage.sourceId() + "[" + consoleMessage.lineNumber() + "]:" + consoleMessage.message());
            return true;
        }
    };

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_top);
        setupWebView();
        setWebViewInformation();
        requestPermission();
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, String[] permissions, int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        if (requestCode == REQUEST_PERMISSION_CODE) {
            IntPredicate p = result -> result == PackageManager.PERMISSION_DENIED;
            if (Arrays.stream(grantResults).anyMatch(p)) {
                Toast.makeText(this, R.string.please_all_grant, Toast.LENGTH_SHORT).show();
                requestPermission();
            } else {
                loadWebView();
            }
        }
    }

    /**
     * WebViewの設定
     */
    @SuppressLint("SetJavaScriptEnabled")
    private void setupWebView() {
        WebView.setWebContentsDebuggingEnabled(true);
        final WebView webView = findViewById(R.id.web_view);
        WebSettings settings = webView.getSettings();
        settings.setJavaScriptEnabled(true);
        settings.setDomStorageEnabled(true);
        settings.setCacheMode(WebSettings.LOAD_NO_CACHE);
        settings.setMediaPlaybackRequiresUserGesture(false);
        webView.setWebChromeClient(webChromeClient);
    }

    /**
     * WebViewで指定のURLを表示
     */
    private void loadWebView() {
        final WebView webView = findViewById(R.id.web_view);
        webView.loadUrl(URL);
    }

    /**
     * 端末にインストールされているWebViewの情報を取得する
     */
    private void setWebViewInformation() {
        List<String> info = new ArrayList<>();
        final WebView webView = findViewById(R.id.web_view);
        info.add("WebView UserAgent " + webView.getSettings().getUserAgentString());
        try {
            PackageInfo pi = getPackageManager().getPackageInfo(WEBVIEW_PACKAGE_ID, 0);
            info.add("WebView VersionName: " + pi.versionName);
            info.add("WebView VersionCode: " + pi.getLongVersionCode());
        } catch (PackageManager.NameNotFoundException e) {
            info.add("WebView Package: [" + WEBVIEW_PACKAGE_ID + "] was not found.");
        }
        final TextView webViewInfo = findViewById(R.id.web_view_info);
        webViewInfo.setText(String.join("\n", info));
    }

    /**
     * 必要なパーミッションのリクエスト
     */
    private void requestPermission() {
        String[] requirePermissions = {Manifest.permission.CAMERA};
        String[] deniedPermissions = Arrays.stream(requirePermissions).filter(p -> checkSelfPermission(p) != PackageManager.PERMISSION_GRANTED).toArray(String[]::new);
        if (deniedPermissions.length == 0) {
            loadWebView();
        } else {
            requestPermissions(deniedPermissions, REQUEST_PERMISSION_CODE);
        }
    }
}