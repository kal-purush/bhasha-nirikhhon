package com.grailcloset.staqueue;

import android.content.Context;
import android.util.AttributeSet;
import android.util.Log;
import android.view.ContextMenu;
import android.view.MenuItem;
import android.webkit.WebView;

/**
 * Created by Oliver on 5/13/2015.
 */
public class MyWebView extends WebView {

    public MyWebView(Context context, AttributeSet attrs) {
        super(context, attrs);
    }

    @Override
    protected void onCreateContextMenu(ContextMenu menu) {
        super.onCreateContextMenu(menu);
//        Log.d("catcat", "LONG CLICK BRUH");

        HitTestResult result = getHitTestResult();

        MenuItem.OnMenuItemClickListener handler = new MenuItem.OnMenuItemClickListener() {
            public boolean onMenuItemClick(MenuItem item) {
                // do the menu action
                return true;
            }
        };

        if (result.getType() == HitTestResult.IMAGE_TYPE ||
                result.getType() == HitTestResult.SRC_IMAGE_ANCHOR_TYPE) {
            // Menu options for an image.
            //set the header title to the image url
            menu.setHeaderTitle(result.getExtra());
            menu.add(0, 1, 0, "Save Image").setOnMenuItemClickListener(handler);
            menu.add(0, 2, 0, "View Image").setOnMenuItemClickListener(handler);
        } else if (result.getType() == HitTestResult.ANCHOR_TYPE ||
                result.getType() == HitTestResult.SRC_ANCHOR_TYPE) {
            // Menu options for a hyperlink.
            //set the header title to the link url
            menu.setHeaderTitle(result.getExtra());
            menu.add(0, 1, 0, "Save Link").setOnMenuItemClickListener(handler);
            menu.add(0, 2, 0, "Share Link").setOnMenuItemClickListener(handler);
        }
    }
}