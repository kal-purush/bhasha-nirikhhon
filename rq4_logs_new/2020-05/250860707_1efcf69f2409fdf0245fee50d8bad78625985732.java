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

import android.content.Context;
import android.net.ConnectivityManager;
import android.net.Network;
import android.net.NetworkCapabilities;
import android.net.NetworkInfo;
import android.os.Build;
import android.util.Log;
import android.widget.Button;
import android.widget.Toast;

import androidx.annotation.NonNull;

import com.android.volley.DefaultRetryPolicy;
import com.android.volley.Request;
import com.android.volley.RequestQueue;
import com.android.volley.toolbox.StringRequest;
import com.android.volley.toolbox.Volley;
import com.google.gson.Gson;
import com.pcchin.studyassistant.R;
import com.pcchin.studyassistant.activity.ActivityConstants;
import com.pcchin.studyassistant.activity.MainActivity;
import com.pcchin.studyassistant.fragment.about.AboutFragment;
import com.pcchin.studyassistant.network.NetworkConstants;

import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

/** Functions used for network related tasks within the app. **/
public final class NetworkFunctions {
    private NetworkFunctions() {
        throw new IllegalStateException("Utility class");
    }

    /** Get the connection status from the connectivity manager. **/
    public static boolean getConnected(ConnectivityManager cm) {
        if (Build.VERSION.SDK_INT < 23) {
            final NetworkInfo ni = cm.getActiveNetworkInfo();
            if (ni != null) {
                return ni.isConnected();
            }
        } else {
            final Network n = cm.getActiveNetwork();
            if (n != null) {
                final NetworkCapabilities nc = cm.getNetworkCapabilities(n);
                if (nc != null) {
                    return nc.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET);
                }
            }
        }
        return false;
    }

    /** Send the feedback / bug report POST request to the server. **/
    public static void sendPostRequest(@NonNull MainActivity activity, String path, @NonNull JSONObject uploadObject,
                                       String sharedPrefValue, Button submitButton) {
        ConnectivityManager cm = (ConnectivityManager)activity.getSystemService(Context.CONNECTIVITY_SERVICE);
        String encryptedString = getServerJson(SecurityFunctions.RSAServerEncrypt(activity, uploadObject.toString()));
        if (encryptedString == null) {
            Log.e(ActivityConstants.LOG_APP_NAME, "Network Error: Unable to encrypt JSON Object using RSA.");
            Toast.makeText(activity, R.string.network_error, Toast.LENGTH_SHORT).show();
        } else if (getConnected(cm)) {
            submitButton.setEnabled(false);
            RequestQueue queue = Volley.newRequestQueue(activity);
            StringRequest postSecBackupFeedback = getStringRequest(activity,
                    NetworkConstants.SEC_BACKUP_API + path,
                    queue, encryptedString, sharedPrefValue, null, submitButton),
                    postBackupFeedback = getStringRequest(activity,
                            NetworkConstants.BACKUP_API + path,
                            queue, encryptedString, sharedPrefValue, postSecBackupFeedback, submitButton),
                    postFeedback = getStringRequest(activity, NetworkConstants.MAIN_API
                            + path, queue, encryptedString,
                            sharedPrefValue, postBackupFeedback, submitButton);
            queue.add(postFeedback);
        } else {
            Toast.makeText(activity, R.string.error_not_connected, Toast.LENGTH_SHORT).show();
        }
    }

    /** Gets the JSON request used to send the feedback / bug report. **/
    @NonNull
    private static StringRequest getStringRequest(MainActivity activity, String fullUrl, RequestQueue queue,
                                                  String encryptedString, String sharedPrefValue,
                                                  StringRequest secondaryRelease, Button submitButton) {
        StringRequest returnRequest = new StringRequest(Request.Method.POST, fullUrl, response -> {
            submitButton.setEnabled(true);
            DataFunctions.storeResponse(activity, sharedPrefValue, response);
            activity.displayFragment(new AboutFragment());
        }, error -> {
            Log.d(ActivityConstants.LOG_APP_NAME, "Network Error: Volley returned error "
                    + error.getMessage() + ":" + error.toString() + " from " + fullUrl + ", stack trace is");
            error.printStackTrace();
            Log.d(ActivityConstants.LOG_APP_NAME, "Attempting to connect to backup server");
            if (secondaryRelease == null) {
                queue.stop();
                Toast.makeText(activity, R.string.network_error, Toast.LENGTH_SHORT).show();
                submitButton.setEnabled(true);
            } else {
                queue.add(secondaryRelease);
            }
        }) {
            @Override
            public Map<String, String> getHeaders() {
                Map<String, String> headers = new HashMap<>();
                headers.put("Accept", "text/*, application/json");
                headers.put("User-agent", NetworkConstants.USER_AGENT);
                return headers;
            }

            @Override
            public String getBodyContentType() {
                return "application/json; charset=utf-8";
            }

            @Override
            public byte[] getBody() {
                return encryptedString.getBytes();
            }
        };
        returnRequest.setRetryPolicy(new DefaultRetryPolicy(
                20 * 1000,
                DefaultRetryPolicy.DEFAULT_MAX_RETRIES,
                DefaultRetryPolicy.DEFAULT_BACKOFF_MULT));
        return returnRequest;
    }

    /** Converts the message into a JSON String with an attribute named message. **/
    private static String getServerJson(String message) {
        return message == null ? null: new Gson().toJson(new PlaceholderJsonClass(message));
    }

    /** Class used for conversion of a String to a class to be parsed by GSON. **/
    private static class PlaceholderJsonClass {
        String message;
        private PlaceholderJsonClass(String message) {
            this.message = message;
        }
    }
}