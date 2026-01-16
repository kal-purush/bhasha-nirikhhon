package com.sun.music_64.data.source.remote;

import android.os.AsyncTask;

import com.sun.music_64.data.model.Track;
import com.sun.music_64.utils.Constants;
import com.sun.music_64.utils.StringUtils;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.HttpsURLConnection;

public class ConnectAPI extends AsyncTask<String, List<Track>, List<Track>> {

    private NetworkCallback mNetworkCallback;

    public ConnectAPI(NetworkCallback networkCallback) {
        this.mNetworkCallback = networkCallback;
    }

    @Override
    protected void onPreExecute() {
        super.onPreExecute();
    }

    @Override
    protected List<Track> doInBackground(String... strings) {
        List<Track> tracks = new ArrayList<>();
        try {
            URL mURL = new URL(strings[0]);
            HttpsURLConnection mHttpConnection = (HttpsURLConnection) mURL.openConnection();
            mHttpConnection.setRequestProperty(Constants.URL_USER_KEY, Constants.URL_USER_VALUE);
            mHttpConnection.setReadTimeout(Constants.URL_REQUEST_TIME);
            mHttpConnection.setConnectTimeout(Constants.URL_REQUEST_TIME);
            mHttpConnection.setRequestMethod(Constants.URL_REQUEST_METHOD);
            mHttpConnection.setDoInput(true);
            mHttpConnection.connect();
            if (mHttpConnection.getResponseCode() == Constants.URL_REQUEST_CODE) {
                InputStream mInputStream = mHttpConnection.getInputStream();
                tracks = readUserJSONFile(mInputStream);
                publishProgress(tracks);
            }
            mHttpConnection.disconnect();
        } catch (IOException | JSONException e) {
            mNetworkCallback.receiverFail(e.getMessage());
        }
        return tracks;
    }

    @Override
    protected void onProgressUpdate(List<Track>... values) {
        mNetworkCallback.receiverSuccess(values[0]);
    }

    @Override
    protected void onPostExecute(List<Track> tracks) {
    }

    private List<Track> readUserJSONFile(InputStream inputStream) throws IOException, JSONException {

        List<Track> tracks = new ArrayList<>();
        String jsonText = readText(inputStream);
        JSONObject JsonObjectMain = new JSONObject(jsonText);
        JSONArray JsonArray = new JSONArray(JsonObjectMain.getString(Constants.URL_COLLECTION));
        JSONObject[] mJSONObject1 = new JSONObject[JsonArray.length()];
        for (int i = 0; i < JsonArray.length(); i++) {
            mJSONObject1[i] = JsonArray.getJSONObject(i);
            JSONObject JSONObjectTrack = new JSONObject(mJSONObject1[i].getString(Constants.URL_TRACK));
            JSONObject JSONObjectUser = new JSONObject(JSONObjectTrack.getString(Constants.URL_USER));
            Track.TrackBuilder trackBuilder = new Track.TrackBuilder();
            trackBuilder.setId(JSONObjectTrack.getInt(Constants.URL_ID));
            trackBuilder.setTitle(JSONObjectTrack.getString(Constants.URL_TITLE));
            trackBuilder.setDuration(JSONObjectTrack.getLong(Constants.URL_DUARTION));
            trackBuilder.setArtist(JSONObjectUser.getString(Constants.URL_USER_NAME));
            trackBuilder.setStreamUrl(StringUtils.initStreamUrl(JSONObjectTrack.getInt(Constants.URL_ID)));
            trackBuilder.setDownloadUrl(StringUtils.initDownloadUrl(JSONObjectTrack.getInt(Constants.URL_ID)));
            trackBuilder.setArtworkUrl(JSONObjectTrack.getString(Constants.URL_ARTWORK));
            trackBuilder.setDownload(JSONObjectTrack.getBoolean(Constants.URL_DOWNLOADABLE));
            tracks.add(trackBuilder.buildTrack());
        }
        return tracks;
    }

    private String readText(InputStream inputStream) throws IOException {
        InputStream is = inputStream;
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        StringBuilder sb = new StringBuilder();
        String s = null;
        while ((s = br.readLine()) != null) {
            sb.append(s);
            sb.append("\n");
        }
        return sb.toString();
    }
}