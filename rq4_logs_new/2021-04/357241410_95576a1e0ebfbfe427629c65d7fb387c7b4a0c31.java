package kr.sswu.croquischallenge.Fragment;

import android.os.Bundle;

import androidx.fragment.app.Fragment;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.webkit.WebView;

import kr.sswu.croquischallenge.R;

public class WebView1Fragment extends Fragment {

    WebView webView;

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {

        View view =  inflater.inflate(R.layout.fragment_web_view1, container, false);

        webView = view.findViewById(R.id.webView1);
        webView.loadUrl("https://line-of-action.com/");

        return view;
    }
}