package com.example.android.newsvision;

import android.app.Dialog;
import android.content.Context;
import android.content.DialogInterface;
import android.os.Bundle;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.support.v4.app.DialogFragment;
import android.support.v7.app.AlertDialog;
import android.view.KeyEvent;
import android.view.LayoutInflater;
import android.view.View;
import android.webkit.WebView;
import android.webkit.WebViewClient;

public class WebViewModalFragment extends DialogFragment {
    OnWebViewClickListener mCallback;
    public static final String ARG_URL = "url";

    private String mUrl;

    public interface OnWebViewClickListener {
        public void onWebViewButtonClick();
    }

    public WebViewModalFragment() {}

    public static WebViewModalFragment newInstance(String url) {
        WebViewModalFragment webFrag = new WebViewModalFragment();
        Bundle args = new Bundle();
        args.putString(ARG_URL, url);
        return  webFrag;
    }

    @Override
    public void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
    }

    @Override
    public void onAttach(Context context) {
        super.onAttach(context);

        // This makes sure that the container activity has implemented
        // the callback interfaces. If not, it throws an exception.
        try {
            mCallback = (OnWebViewClickListener) context;
        } catch (ClassCastException e) {
            throw new ClassCastException(context.toString()
                    + " must implement OnWebViewClickListener");
        }
    }

    public void setUrl(String url) {
        this.mUrl = url;
    }

    @NonNull
    @Override
    public Dialog onCreateDialog(Bundle savedInstanceState) {
        AlertDialog.Builder builder = new AlertDialog.Builder(getActivity());

        // Get LayoutInflater
        LayoutInflater inflater = getActivity().getLayoutInflater();

        View rootView = inflater.inflate(R.layout.fragment_webview,null);

        // Sets up the WebView
        final WebView mWebView = rootView.findViewById(R.id.nyt_webview);

        if (mUrl != null) {
            mWebView.loadUrl(mUrl);
        }

        // Creates WebView client to handle page navigation
        mWebView.setWebViewClient(new WebViewClient() {
            @Override
            public boolean shouldOverrideUrlLoading(WebView view, String url) {
                view.loadUrl(url);
                return true;
            }

        });

        // Handle back navigation in the WebView
        mWebView.setOnKeyListener(new View.OnKeyListener() {
            @Override
            public boolean onKey(View view, int keyCode, KeyEvent keyEvent) {
                if (keyCode == KeyEvent.KEYCODE_BACK && mWebView.canGoBack()) {
                    mWebView.goBack();
                    return true;
                }
                return false;
            }
        });

        builder.setView(mWebView);
        builder.setNegativeButton(R.string.webview_close_message, new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialogInterface, int i) {
                dialogInterface.dismiss();
            }
        });

        builder.setPositiveButton(R.string.webview_search, new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialogInterface, int i) {
                dialogInterface.dismiss();
                mCallback.onWebViewButtonClick();
            }
        });

        return builder.create();

    }
}