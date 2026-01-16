/*
 * Copyright 2020 PC Chin. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pcchin.studyassistant.functions;

import android.app.Activity;
import android.content.Context;
import android.content.SharedPreferences;
import android.util.Log;
import android.widget.Toast;

import androidx.annotation.NonNull;

import com.pcchin.studyassistant.activity.ActivityConstants;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;

/** Functions used for processing data throughout the app. **/
public final class DataFunctions {
    private DataFunctions() {
        throw new IllegalStateException("Utility class");
    }

    /** Gets all the issue numbers from a specific shared preference value. **/
    public static ArrayList<Integer> getAllResponses(@NonNull Activity activity, String sharedPrefValue) {
        ArrayList<Integer> returnArray = ConverterFunctions.jsonToSingleIntegerArray(
                activity.getSharedPreferences(activity.getPackageName(), Context.MODE_PRIVATE)
                        .getString(sharedPrefValue, ""));
        if (returnArray == null) {
            return null;
        } else {
            Collections.sort(returnArray);
            return returnArray;
        }
    }

    /** Store the number of the created issue into the shared preferences. **/
    public static void storeResponse(@NonNull Activity activity, String sharedPrefValue, String jsonResponse) {
        try {
            JSONObject object = new JSONObject(jsonResponse);
            int issueNum = object.getInt("number");
            SharedPreferences sharedPref = activity.getSharedPreferences(activity.getPackageName(), Context.MODE_PRIVATE);
            ArrayList<Integer> issueList = ConverterFunctions.jsonToSingleIntegerArray(sharedPref.getString(sharedPrefValue, ""));
            if (issueList == null) issueList = new ArrayList<>();
            if (!issueList.contains(issueNum)) issueList.add(issueNum);
            SharedPreferences.Editor editor = sharedPref.edit();
            editor.putString(sharedPrefValue, ConverterFunctions.singleIntArrayToJson(issueList));
            editor.apply();
            Toast.makeText(activity, "Issue " + issueNum + " created", Toast.LENGTH_SHORT).show();
        } catch (JSONException e) {
            Log.w(ActivityConstants.LOG_APP_NAME, "Network Error: Unable to parse response " +
                    "from feedback submission, error is");
            e.printStackTrace();
        }
    }

    /** Removes a specific issue numbers of the created issue from the shared preferences. **/
    public static void removeResponse(Activity activity, String sharedPrefValue,
                                      int issueNum) {
        ArrayList<Integer> issueNumList = new ArrayList<>();
        issueNumList.add(issueNum);
        removeResponse(activity, sharedPrefValue, issueNumList);
    }

    /** Removes a list of issue numbers of the created issue from the shared preferences. **/
    private static void removeResponse(@NonNull Activity activity, String sharedPrefValue,
                                       ArrayList<Integer> issueNumList) {
        SharedPreferences sharedPref = activity.getSharedPreferences(activity.getPackageName(), Context.MODE_PRIVATE);
        ArrayList<Integer> issueList = ConverterFunctions.jsonToSingleIntegerArray(sharedPref.getString(sharedPrefValue, ""));
        if (issueList == null) issueList = new ArrayList<>();
        // Removal by object, not by index
        for (int issueNum: issueNumList) {
            if (issueList.contains(issueNum)) issueList.remove((Integer) issueNum);
        }
        SharedPreferences.Editor editor = sharedPref.edit();
        editor.putString(sharedPrefValue, ConverterFunctions.singleIntArrayToJson(issueList));
        editor.apply();
    }
}