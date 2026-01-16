package com.baeflower.sol.tourlist_gallerysample.util;

import android.content.Context;
import android.content.res.Resources;
import android.graphics.Bitmap;
import android.graphics.drawable.BitmapDrawable;
import android.graphics.drawable.Drawable;
import android.net.Uri;
import android.os.AsyncTask;
import android.widget.ImageView;

import java.lang.ref.WeakReference;

/**
 * Created by sol on 2015-04-09.
 */
public class DynamicBitmapLoading {

    private BitmapScaleSetting mBitmapScaleSetting;
    private Context mContext;
    private TourImageCache mTourImageCache;


    /**
     * 생성자
     * @param mContext
     */
    public DynamicBitmapLoading(Context mContext) {
        this.mContext = mContext;
        mTourImageCache = TourImageCache.getInstance();
    }

    /**
     * BitmapWorkerTask를 execute 하기 전에 AsyncDrawable 인스턴스를 생성하여 Bitmap을 보여줄 imageView와 묶는다
     *
     * @param uri
     * @param imageView
     */
    public void loadBitmap(Uri uri, ImageView imageView, BitmapScaleSetting bitmapScaleSetting) {

        final String imageKey = uri.getPath();
        final Bitmap bitmap = getBitmapFromMemCache(imageKey);

        if (bitmap != null) {
            imageView.setImageBitmap(bitmap);
        } else {
            if (cancelPotentialWork(uri, imageView)) {
                final BitmapWorkerTask task = new BitmapWorkerTask(imageView, bitmapScaleSetting);

                // AsyncDrawable 인스턴스와 imageView binding
                final AsyncDrawable asyncDrawable = new AsyncDrawable(mContext.getResources(), null, task);
                imageView.setImageDrawable(asyncDrawable);
                task.execute(uri);
            }
        }
    }

    // BitmapDrawable은 Task가 종료될 때 까지 출력될 임시 이미지를 보여주기 위해 사용
    static class AsyncDrawable extends BitmapDrawable {
        private final WeakReference<BitmapWorkerTask> bitmapWorkerTaskReference;

        public AsyncDrawable(Resources res, Bitmap bitmap, BitmapWorkerTask bitmapWorkerTask) {
            super(res, bitmap);
            bitmapWorkerTaskReference = new WeakReference<BitmapWorkerTask>(bitmapWorkerTask);
        }

        public BitmapWorkerTask getBitmapWorkerTask() {
            return bitmapWorkerTaskReference.get();
        }
    }


    /**
     * imageView가 running task와 연관되어 있는지 확인. 연관있으면 cancel()을 통해 이전 task 종료
     *
     * @param uri
     * @param imageView
     * @return
     */
    public static boolean cancelPotentialWork(Uri uri, ImageView imageView) {
        final BitmapWorkerTask bitmapWorkerTask = getBitmapWorkerTask(imageView);

        if (bitmapWorkerTask != null) {
            final Uri bitmapData = bitmapWorkerTask.getmUri();
            // If bitmapData is not yet set or it differs from the new data
            if (bitmapData == null || bitmapData != uri) {
                // Cancel previous task
                bitmapWorkerTask.cancel(true);
            } else {
                // The same work is already in progress
                return false;
            }
        }
        // No task associated with the ImageView, or an existing task was cancelled
        return true;
    }


    /*
        imageView와 연관된 task를 가져오기 위해.
        imageView와 imageView의 drawable이 null이 아닐경우 Workertask 반환
    */
    private static BitmapWorkerTask getBitmapWorkerTask(ImageView imageView) {
        if (imageView != null) {
            final Drawable drawable = imageView.getDrawable();
            if (drawable instanceof AsyncDrawable) { //drawable 이 null일 경우 false 반환
                final AsyncDrawable asyncDrawable = (AsyncDrawable) drawable;
                return asyncDrawable.getBitmapWorkerTask();
            }
        }
        return null;
    }

    public void addBitmapToMemoryCache(String key, Bitmap bitmap) {
        if (getBitmapFromMemCache(key) == null) {
            mTourImageCache.getmMemoryCache().put(key, bitmap);
        }
    }

    public Bitmap getBitmapFromMemCache(String key) {
        return mTourImageCache.getmMemoryCache().get(key);
    }


    class BitmapWorkerTask extends AsyncTask<Uri, Void, Bitmap> {

        private Uri mUri;
        private final WeakReference<ImageView> imageViewReference;

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

            // bitmap decode
            mBitmapScaleSetting.copyUriToFile(mUri);
            Bitmap bitmap = mBitmapScaleSetting.loadImageWithSampleSize();
            bitmap = mBitmapScaleSetting.resizeImageWithinBoundary(bitmap);

            // cache 저장
            addBitmapToMemoryCache(mUri.getPath(), bitmap);

            return bitmap;
        }

        @Override
        protected void onPostExecute(Bitmap bitmap) {
            if (isCancelled()) {
                bitmap = null;
            }

            if (imageViewReference != null && bitmap != null) {
                final ImageView imageView = imageViewReference.get();
                final BitmapWorkerTask bitmapWorkerTask = getBitmapWorkerTask(imageView);
                if (this == bitmapWorkerTask && imageView != null) {
                    imageView.setImageBitmap(bitmap);
                }
            }
        }

        public Uri getmUri() {
            return mUri;
        }
    }

}