package com.solonari.igor.virtualshooter;

import android.os.AsyncTask;
import android.os.Handler;
import android.util.Log;

/**
 * Created by isolo on 1/28/2017.
 * AsyncTask class which manages connection with server app and is sending command.
 */
public class ServerTask extends AsyncTask<String, String, TCPClient> {

    private static final String COMMAND = "test";
    private TCPClient tcpClient;
    private Handler mHandler;
    private static final String TAG = "ServerTask";

    /**
     * ShutdownAsyncTask constructor with handler passed as argument. The UI is updated via handler.
     * In doInBackground(...) method, the handler is passed to TCPClient object.
     * @param mHandler Handler object that is retrieved from MainActivity class and passed to TCPClient
     *                 class for sending messages and updating UI.
     */
    public ServerTask(Handler mHandler){
        this.mHandler = mHandler;
    }

    /**
     * Overriden method from AsyncTask class. There the TCPClient object is created.
     * @param params From MainActivity class empty string is passed.
     * @return TCPClient object for closing it in onPostExecute method.
     */
    @Override
    protected TCPClient doInBackground(String... params) {
        Log.d(TAG, "In do in background");

        try{
            tcpClient = new TCPClient(mHandler,
                    COMMAND,
                    "178.168.41.5",
                    new TCPClient.MessageCallback() {
                        @Override
                        public void callbackMessageReceiver(String message) {
                            publishProgress(message);
                        }
                    });

        }catch (NullPointerException e){
            Log.d(TAG, "Caught null pointer exception");
            e.printStackTrace();
        }
        tcpClient.run();
        return null;
    }

    /**
     * Overriden method from AsyncTask class. Here we're checking if server answered properly.
     * @param values If "restart" message came, the client is stopped and computer should be restarted.
     *               Otherwise "wrong" message is sent and 'Error' message is shown in UI.
     */
    @Override
    protected void onProgressUpdate(String... values) {
        super.onProgressUpdate(values);
        Log.d(TAG, "In progress update, values: " + values.toString());
        if(values[0].equals("shutdown")){
            tcpClient.sendMessage(COMMAND);
            tcpClient.stopClient();
            mHandler.sendEmptyMessageDelayed(map.SHUTDOWN, 2000);

        }else{
            tcpClient.sendMessage("wrong");
            mHandler.sendEmptyMessageDelayed(map.ERROR, 2000);
            tcpClient.stopClient();
        }
    }

    @Override
    protected void onPostExecute(TCPClient result){
        super.onPostExecute(result);
        Log.d(TAG, "In on post execute");
        if(result != null){
            result.stopClient();
        }
        mHandler.sendEmptyMessageDelayed(map.SENT, 4000);

    }
}