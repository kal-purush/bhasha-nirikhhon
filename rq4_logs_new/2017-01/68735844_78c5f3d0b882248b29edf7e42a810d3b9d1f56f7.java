package com.artback.bth.locationtimer.Calendar;

import android.content.Context;
import android.preference.PreferenceManager;

import com.google.api.client.extensions.android.http.AndroidHttp;
import com.google.api.client.googleapis.extensions.android.gms.auth.GoogleAccountCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.services.calendar.Calendar;
import com.google.api.services.calendar.CalendarScopes;

import java.util.Arrays;


public class CredentialHandler {
    private static CredentialHandler instance;
    private com.google.api.services.calendar.Calendar mService ;
    public GoogleAccountCredential mCredential;
    public String accountName;
    public static final String PREF_ACCOUNT_NAME = "accountName";
    private static final String[] SCOPES = { CalendarScopes.CALENDAR};
    public CredentialHandler(Context context){
        accountName = PreferenceManager.getDefaultSharedPreferences(context)
                .getString(PREF_ACCOUNT_NAME, null);
        mCredential = GoogleAccountCredential.usingOAuth2(
                context, Arrays.asList(SCOPES))
                .setBackOff(new ExponentialBackOff());
        if(accountName != null) {
            mCredential.setSelectedAccountName(accountName);
            HttpTransport transport = AndroidHttp.newCompatibleTransport();
            JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
            mService = new com.google.api.services.calendar.Calendar.Builder(
                    transport, jsonFactory, mCredential).setApplicationName("LocationTimer").build();
        }
    }
    public Calendar getService(){
       return mService;
    }
}