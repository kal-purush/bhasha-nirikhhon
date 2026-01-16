package com.okandroid.boot.lang;

import android.content.Context;
import android.net.Uri;
import android.support.v4.content.FileProvider;

import com.okandroid.boot.AppContext;

import java.io.File;

/**
 * Created by idonans on 2017/3/27.
 */

public class BootFileProvider extends FileProvider {

    public static Uri getUriForFile(File file) {
        Context context = AppContext.getContext();
        final String authority = context.getPackageName() + ".bootfileprovider";
        return FileProvider.getUriForFile(context, authority, file);
    }

}