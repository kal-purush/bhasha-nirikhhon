package com.example.pacmanlike.view;

import android.content.ClipData;
import android.content.ClipboardManager;
import android.content.Context;

import com.example.pacmanlike.main.AppConstants;

public class ImportExportMap {
    public static String importMap(String level){
        String newLines = level.replace(AppConstants.EXPORT_NEW_LINE, AppConstants.CHAR_CSV_NEWLINE);
        String delimitersAlso = newLines.replace(AppConstants.EXPORT_CSV_DELIMITER, AppConstants.CHAR_CSV_DELIMITER);

        return delimitersAlso;
    }

    public static String exportMap(String level){
        String noNewLines = level.replace(AppConstants.CHAR_CSV_NEWLINE, AppConstants.EXPORT_NEW_LINE);
        String noDelimiters = noNewLines.replace(AppConstants.CHAR_CSV_DELIMITER, AppConstants.EXPORT_CSV_DELIMITER);

        return noDelimiters;
    }

    public static void copyToClipboard(Context context, String string){
        ClipboardManager clipboard = (ClipboardManager) context.getSystemService(Context.CLIPBOARD_SERVICE);
        ClipData clip = ClipData.newPlainText(AppConstants.CLIPBOARD_LABEL, string);
        clipboard.setPrimaryClip(clip);
    }
}