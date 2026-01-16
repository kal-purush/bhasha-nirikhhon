package edu.pdx.team_b_capstone2015.s_pi_watch;

import android.os.Bundle;
import android.util.Log;

import com.google.android.gms.common.api.GoogleApiClient;
import com.google.android.gms.wearable.DataApi;
import com.google.android.gms.wearable.DataEvent;
import com.google.android.gms.wearable.DataEventBuffer;

import com.google.android.gms.wearable.DataMap;
import com.google.android.gms.wearable.PutDataMapRequest;
import com.google.android.gms.wearable.Wearable;
import com.google.android.gms.wearable.WearableListenerService;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class MobileListenerService extends WearableListenerService
        implements GoogleApiClient.ConnectionCallbacks {

    public final static String apiURL = "http://api.s-pi-demo.com/patients";
    public static final String FIELD_QUERY_ON = "query_on";
    public static final String PATH_PATIENTS = "/patients";
    public static final String PATH_PATIENT = "/patient";
    public static final String PATH_QUERY_STATUS = "/query_status";

    //REST API keys
    private static final String NAME = "name";
    private static final String BED = "bed" ;
    private static final String ID = "id";
    private static final String AGE = "age";
    private static final String TEMP = "temperature";
    private static final String HEIGHT = "height";
    private static final String BP = "blood_pressure" ;
    private static final String STATUS = "status";
    private static final String CASE_ID = "case_id" ;
    private static final String H_ID = "hospital_admission_id" ;
    private static final String CARDIAC = "cardiac";
    private static final String ALLERGIES = "allergies";
    private static final String WEIGHT = "weight";
    private static final String HEART_RATE = "heart-rate";
    private static final String P_ID= "patient_id";

    private GoogleApiClient mGoogleApiClient;
    private static final String TAG = "SPIMobileService";

    @Override
    public void onCreate() {
        super.onCreate();
        mGoogleApiClient = new GoogleApiClient.Builder(this)
                .addApi(Wearable.API)
                .addConnectionCallbacks(this)
                .build();
        Log.d(TAG, "onCreate");
    }

    @Override
    public void onDestroy() {
        super.onDestroy();
        Log.d(TAG, "onDestroy");
        if (mGoogleApiClient.isConnected() || mGoogleApiClient.isConnecting()) {
            mGoogleApiClient.disconnect();
        }

    }

    @Override
    public void onDataChanged(DataEventBuffer dataEvents) {
        if (Log.isLoggable(TAG, Log.DEBUG)) {
            Log.d(TAG, "onDataChanged: " + dataEvents + " for " + getPackageName());
        }
        for (DataEvent event : dataEvents) {
            Log.d(TAG, "URI path: " + event.getDataItem().getUri().getPath().toString());

            if (event.getType() == DataEvent.TYPE_DELETED) {
                Log.i(TAG, event + " deleted");
            } else if (event.getType() == DataEvent.TYPE_CHANGED) {//Indicates that the enclosing DataEvent was triggered by a data item being added or changed.
                //get a data item from the data map

                if(event.getDataItem().getUri().getPath().toString().contentEquals(PATH_QUERY_STATUS)){
                    Log.i(TAG,"query status changed");
                    Boolean queryServer = DataMap.fromByteArray(event.getDataItem().getData()).get(FIELD_QUERY_ON);
                    if (queryServer) {
                        Log.d(TAG, "start querying server");
                        String json = getPatients();
                        if(json != null){
                            updateData(event,json);
                        }
                        // and set up system to regularly query the other data
                        // this needs to be in another service.
                    } else {
                        Log.d(TAG, "stop querying server");
                        // stop querying the server for updates.
                    }
                }
                if(event.getDataItem().getUri().getPath().toString().contentEquals(PATH_PATIENTS)){
                    Log.i(TAG, "Patient data changed");
                }
            }
        }
    }
    //queries the http REST api for patient info
    //retuns a json string with all patient info
    private String getPatients() {
        Log.d(TAG, "getting patient names");
        String urlString=apiURL;
        String result;
        InputStream in;
        // HTTP Get
        try {
            URL url = new URL(urlString);
            HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
            in = urlConnection.getInputStream();

        } catch (Exception ex ) {
            Log.e(TAG, "Error during HTTP connection");
            return null;
        }
        result = convertStreamToString(in);
        Log.d(TAG, "server returned from patients call " + result);
        return result;
    }
    //converts a stream to a string
    String convertStreamToString(java.io.InputStream is) {
        java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }
    //updates the shared dataitems
    //takes a json string
    private void updateData(DataEvent e, String result){
        if (mGoogleApiClient.isConnected()) {
            Log.d(TAG, "Already connected");
        } else if (!mGoogleApiClient.isConnecting()) {
            mGoogleApiClient.blockingConnect();
            Log.d(TAG, "connected via blockingConnect");
        }

        PutDataMapRequest put = PutDataMapRequest.create(PATH_PATIENTS);
        put.getDataMap().putString("json", result);//json string of all patient data
        put.getDataMap().putString("event", this.toString());//forces update of patients
        DataApi.DataItemResult status =
                Wearable.DataApi.putDataItem(mGoogleApiClient, put.asPutDataRequest()).await();
        Log.d(TAG, "Put results: " + status.getStatus().toString());
        //send each patient

        for(PutDataMapRequest p : parseJS(result)){
            Log.d(TAG, "Puting " + p.getUri().getPath().toString());
            Wearable.DataApi.putDataItem(mGoogleApiClient, p.asPutDataRequest()).await();
        }

    }

    @Override
    public void onConnected(Bundle bundle) {
        //Log.d(TAG, "onConnected");
    }

    @Override
    public void onConnectionSuspended(int i) {
        //Log.d(TAG, "onConnectionSuspended");
    }

    //parses the json and returns a list of PutDataMaps for each patient with the path set to patient#
    private List<PutDataMapRequest> parseJS(String result) {
        ArrayList<PutDataMapRequest> putDataMapRequest = new ArrayList<>();
        DataMap data;
        //Parse json
        //Create JSON object
        JSONObject jObject, patient;
        try {
            jObject = new JSONObject(result);
            for (int i = 1; i < 5; i++){
                patient = new JSONObject(jObject.getString((new Integer(i)).toString()));
                PutDataMapRequest put = PutDataMapRequest.create(PATH_PATIENT+Integer.toString(i));
                put.getDataMap().putString(NAME, patient.getString(NAME));
                put.getDataMap().putString(BED, patient.getString(BED));
                put.getDataMap().putString(ID, patient.getString(ID));
                put.getDataMap().putString(AGE, patient.getString(AGE));
                put.getDataMap().putString(TEMP, patient.getString(TEMP));
                put.getDataMap().putString(HEIGHT, patient.getString(HEIGHT));
                put.getDataMap().putString(WEIGHT, patient.getString(WEIGHT));
                put.getDataMap().putString(BP, patient.getString(BP));
                put.getDataMap().putString(HEART_RATE, patient.getString(HEART_RATE));
                put.getDataMap().putString(STATUS, patient.getString(STATUS));
                put.getDataMap().putString(CASE_ID, patient.getString(CASE_ID));
                put.getDataMap().putString(H_ID, patient.getString(H_ID));
                put.getDataMap().putString(CARDIAC, patient.getString(CARDIAC));
                put.getDataMap().putString(ALLERGIES, patient.getString(ALLERGIES));
                put.getDataMap().putString(P_ID, patient.getString(P_ID));
                putDataMapRequest.add(put);
            }

        } catch (JSONException e) {
            Log.d(TAG, "JSON ERROR");
            e.printStackTrace();
            return null;
        }
        return putDataMapRequest;
    }

}