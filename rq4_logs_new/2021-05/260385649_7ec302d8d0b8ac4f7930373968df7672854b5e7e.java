package com.mzelzoghbi.zgallery.util;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import java.io.File;

public class Utils {
    /**
     * @param url
     * @return
     */
    @NonNull
    public static String getFileName(@Nullable String url) {
        if (url == null || url.trim().isEmpty())
            return "";

        int pos = url.lastIndexOf(File.separatorChar);
        if (pos == -1) return url;
        return url.substring(pos + 1).replaceAll(" ", "");
    }
}