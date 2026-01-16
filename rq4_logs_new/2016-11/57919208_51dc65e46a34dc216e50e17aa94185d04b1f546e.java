package com.PrivacyGuard.Plugin;

import android.content.Context;
import android.util.Log;
import android.widget.Toast;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

/**
 * Created by w3kim on 2016-11-08.
 */

public class KeywordDetection implements IPlugin {

    private static final String TAG = KeywordDetection.class.getSimpleName();
    private static final String KEYWORDS_FILE_NAME = "keywords.txt";

    private static boolean init = false;
    private static final Set<String> keywords = new HashSet<>();

    @Override
    public LeakReport handleRequest(String request) {
        ArrayList<LeakInstance> leaks = new ArrayList<>();

        for (String keyword : keywords) {
            if (request.contains(keyword)) {
                leaks.add(new LeakInstance("Keyword", keyword));
            }
        }

        if(leaks.isEmpty()){
            return null;
        }

        LeakReport rpt = new LeakReport(LeakReport.LeakCategory.KEYWORD);
        rpt.addLeaks(leaks);
        return rpt;
    }

    @Override
    public LeakReport handleResponse(String response) {
        return null;
    }

    @Override
    public String modifyRequest(String request) {
        return request;
    }

    @Override
    public String modifyResponse(String response) {
        return response;
    }

    @Override
    public void setContext(Context context) {
        if (init) return;
        // reading an up-to-date keywords
        File src = new File(context.getFilesDir() + "/" + KEYWORDS_FILE_NAME);
        if (src.exists()) {
            keywords.clear();
            try {
                Scanner scanner = new Scanner(new FileInputStream(src));
                while (scanner.hasNextLine()) {
                    keywords.add(scanner.nextLine());
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            Log.d(TAG, "Keywords refreshed; " + keywords.size() + " keywords are registered");
        }
        init = true;
    }

    public static void invalidate() {
        init = false;
    }
}