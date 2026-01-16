package com.metafour.cwbay.process;

import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.os.AsyncTask;
import android.util.Log;
import android.widget.ImageView;

import java.io.InputStream;
import java.net.URL;

/**
 * Created by Noor on 1/30/2015.
 */
public class ImageDownloadTask extends AsyncTask<String, Void, Bitmap> {

    private final String imageURL;
    private final ImageView imageView;
    private final Context context;

    /**
     * Constructor
     * @param context
     * @param imageURL
     * @param imageView
     */
    public ImageDownloadTask(final Context context, final String imageURL, final ImageView imageView) {
        this.imageURL = imageURL;
        this.imageView = imageView;
        this.context = context;
    }

    @Override
    protected Bitmap doInBackground(final String... args) {
        Bitmap bitmap = null;

        try {

            final InputStream in = new URL(imageURL).openStream();
            bitmap = BitmapFactory.decodeStream(in);
            in.close();

        } catch (final Exception e) {
            Log.e("Error", e.getMessage());
        }
        return bitmap;
    }


    @Override
    protected void onPostExecute(final Bitmap result) {
        if(result != null){
            imageView.setImageBitmap(result);
        }
    }
}