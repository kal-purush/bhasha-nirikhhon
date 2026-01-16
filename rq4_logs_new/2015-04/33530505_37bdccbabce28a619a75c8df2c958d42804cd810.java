package com.baeflower.sol.tourlist_gallerysample;

import android.graphics.Bitmap;
import android.net.Uri;
import android.os.AsyncTask;
import android.widget.ImageView;

import com.baeflower.sol.tourlist_gallerysample.model.TourImage;
import com.baeflower.sol.tourlist_gallerysample.util.BitmapScaleSetting;

import java.lang.ref.WeakReference;

/**
 * Created by sol on 2015-04-08.
 */

public class BitmapWorkerTask extends AsyncTask<Uri, Void, Bitmap> {

    private final WeakReference<ImageView> imageViewReference;

    private Uri mUri;
    private String mPath;
    private BitmapScaleSetting mBitmapScaleSetting;
    private TourImage mTourImage;

    public BitmapWorkerTask(ImageView imageView, BitmapScaleSetting bitmapScaleSetting) {
        // Use a WeakReference to ensure the ImageView can be garbage collected

        imageViewReference = new WeakReference<>(imageView);
        mBitmapScaleSetting = bitmapScaleSetting;

    }

    // Dynamic Loading
    // Decode image in background.
    @Override
    protected Bitmap doInBackground(Uri... params) {
        mUri = params[0];
        mPath = mUri.getPath();

        // bitmap decode
        mBitmapScaleSetting.copyUriToFile(mUri);
        Bitmap bitmap = mBitmapScaleSetting.loadImageWithSampleSize();
        bitmap = mBitmapScaleSetting.resizeImageWithinBoundary(bitmap);

        return bitmap;
    }

    @Override
    protected void onPostExecute(Bitmap bitmap) {
        if (imageViewReference != null && bitmap != null) {
            final ImageView imageView = imageViewReference.get();
            if (imageView != null) {
                mTourImage = new TourImage(mPath, bitmap);
                imageView.setImageBitmap(bitmap);
            }
        }
    }


    public Uri getmUri() {
        return mUri;
    }
}