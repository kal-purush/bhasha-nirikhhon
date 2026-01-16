package com.example.android.navigationdrawerexample;

import android.content.Context;
import android.content.DialogInterface;
import android.content.Intent;
import android.os.Bundle;
import android.view.View;

/**
 * Created by Jinchengliu on 11/5/16.
 */

public class ClickWebViewListener implements View.OnClickListener {
    private  String webUrl;
    private Context c;
    public ClickWebViewListener(Context c,String webUrl)
    {
        this.webUrl=webUrl;
        this.c=c;
    }
    @Override
    public void onClick(View v)
    {
        Intent intent=new Intent(c,OpenWebView.class);
        Bundle bundle=new Bundle();
        bundle.putString("url",webUrl);
        intent.putExtras(bundle);
        c.startActivity(intent);
    }
}