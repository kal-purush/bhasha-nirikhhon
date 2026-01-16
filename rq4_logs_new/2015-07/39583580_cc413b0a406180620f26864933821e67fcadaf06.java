package nemesis.BD;

import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.os.AsyncTask;

import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by inigo on 30/07/2015.
 */

public class ObtenerFotoInternet extends AsyncTask<Void, Void, Void> {
     public String url ="";
    public  Bitmap imagen=null;
    public ObtenerFotoInternet(String url ){
        this.url=url;
    }


    @Override
    protected Void doInBackground(Void... params) {
        try{
            URL imageUrl = new URL(url);
            HttpURLConnection conn = (HttpURLConnection) imageUrl.openConnection();
            conn.connect();
            imagen = BitmapFactory.decodeStream(conn.getInputStream());
        }catch (Exception a){}
        cancel(true);
        return null;
    }
    @Override

    protected void onPreExecute() {
    }






}