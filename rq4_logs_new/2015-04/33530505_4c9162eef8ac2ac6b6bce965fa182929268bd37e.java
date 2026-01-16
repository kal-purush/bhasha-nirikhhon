package com.baeflower.sol.tourlist_gallerysample;


import android.app.Activity;
import android.content.Intent;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.net.Uri;
import android.os.Bundle;
import android.os.Environment;
import android.provider.MediaStore;
import android.support.v7.app.ActionBarActivity;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.GridView;

import com.baeflower.sol.tourlist_gallerysample.adapter.GridAdapter;
import com.baeflower.sol.tourlist_gallerysample.filecache.FileCache;
import com.baeflower.sol.tourlist_gallerysample.filecache.FileCacheFactory;
import com.baeflower.sol.tourlist_gallerysample.imagecache.ImageCache;
import com.baeflower.sol.tourlist_gallerysample.imagecache.ImageCacheFactory;
import com.baeflower.sol.tourlist_gallerysample.model.TourImage;
import com.baeflower.sol.tourlist_gallerysample.util.Constant;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class GalleryActivity extends ActionBarActivity implements View.OnClickListener {

    // 상수
    private static final String TAG = GalleryActivity.class.getSimpleName();
    private static final int SELECT_FROM_GALLERY = 1;

    // Resource
    private Button mBtnGetImg;
    private GridView mGridView;

    // 폰에서 로딩할 수 있는 최대 픽셀 수(ex. 갤S4 : 4096)
    private int mImageSizeBoundary;

    // Data
    private GridAdapter mGridAdapter;
    private List<TourImage> mImgList;
    private List<String> mPathList;


    // Cache
    private FileCache mFileCache;
    private ImageCache mImageCache;

    private void init() {
        mBtnGetImg = (Button) findViewById(R.id.btn_get_img);
        mGridView = (GridView) findViewById(R.id.gv_each_image);

        // Data
        mImgList = new ArrayList<>();
        mPathList = new ArrayList<>();
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_gallery);

        init();

        // Cache
        // Get max available VM memory, exceeding this amount will throw an
        // OutOfMemory exception. Stored in kilobytes as LruCache takes an
        // int in its constructor.
        final int maxMemory = (int) (Runtime.getRuntime().maxMemory() / 1024);

        // Use 1/8th of the available memory for this memory cache.
        final int cacheSize = maxMemory / 8;

        String cacheName = "gallery";
        FileCacheFactory.initialize(this);
        if (!FileCacheFactory.getInstance().has(cacheName)) {
            FileCacheFactory.getInstance().create(cacheName, cacheSize);
        }
        mFileCache = FileCacheFactory.getInstance().get(cacheName);

        // 이미지 캐시 초기화
        int memoryImageMaxCounts = 20;
        ImageCacheFactory.getInstance().createTwoLevelCache(cacheName, memoryImageMaxCounts);
        mImageCache = ImageCacheFactory.getInstance().get(cacheName);


        // Adapter
        // View
        mGridAdapter = new GridAdapter(getApplicationContext(), mImgList, mImageCache);
        mGridView.setAdapter(mGridAdapter);

        setmImageSizeBoundary(Constant.getMaxTextureSize());
        mBtnGetImg.setOnClickListener(this);

    } // onCreate

    @Override
    public void onClick(View v) {
        Intent intent = new Intent(Intent.ACTION_PICK,
                android.provider.MediaStore.Images.Media.EXTERNAL_CONTENT_URI);
        intent.setType(MediaStore.Images.Media.CONTENT_TYPE);
        startActivityForResult(intent, SELECT_FROM_GALLERY);

    } // onClick

    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        // Toast.makeText(getApplicationContext(), "resultCode : " + resultCode,
        // Toast.LENGTH_SHORT).show();

        if (requestCode == SELECT_FROM_GALLERY) {
            if (resultCode == Activity.RESULT_OK) {
                Uri uri = data.getData(); // Received Data from the intent
                String path = uri.getPath();

                Log.d(TAG, "path : " + path);

                copyUriToFile(uri, getTempImageFile());
                addBitmapToImgList(path);
                // addBitmapToImgListUsingCache(path); // cache 사용
                mGridAdapter.setmShowBtns(false);
                mGridAdapter.notifyDataSetChanged();
            }
        }
    }

    public void addBitmapToImgListUsingCache(String path) {

        // 이미지 캐시 사용 부분
        Bitmap bitmapInCache = mImageCache.getBitmap(path);

        if (bitmapInCache == null) {
            Bitmap loadedBitmap = loadImageWithSampleSize(getTempImageFile());
            loadedBitmap = resizeImageWithinBoundary(loadedBitmap);
            bitmapInCache = addBitmapToCache(path, loadedBitmap);
        }

        TourImage tourImage = new TourImage(path, bitmapInCache);
        mImgList.add(tourImage);
    }

    private Bitmap addBitmapToCache(String path, Bitmap bitmap) {
        mImageCache.addBitmap(path, bitmap);
        return mImageCache.getBitmap(path);
    }

    private File getTempImageFile() {
        File path = new File(Environment.getExternalStorageDirectory() + "/Android/data/"
                + getPackageName() + "/temp/");
        if (!path.exists()) {
            path.mkdirs();
        }
        File file = new File(path, "tempImage.png");
        return file;
    }

    private void copyUriToFile(Uri srcUri, File target) {
        FileInputStream inputStream = null;
        FileOutputStream outputStream = null;
        FileChannel fcin = null;
        FileChannel fcout = null;
        try {
            // 스트림 생성
            inputStream = (FileInputStream) getContentResolver().openInputStream(srcUri);
            outputStream = new FileOutputStream(target);

            // 채널 생성
            fcin = inputStream.getChannel();
            fcout = outputStream.getChannel();

            // 채널을 통한 스트림 전송
            long size = fcin.size();
            fcin.transferTo(0, size, fcout);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                fcout.close();
            } catch (IOException ioe) {
            }
            try {
                fcin.close();
            } catch (IOException ioe) {
            }
            try {
                outputStream.close();
            } catch (IOException ioe) {
            }
            try {
                inputStream.close();
            } catch (IOException ioe) {
            }
        }
    }

    private void addBitmapToImgList(String path) {
        // sample size 를 적용하여 bitmap load.
        Bitmap bitmap = loadImageWithSampleSize(getTempImageFile());
        // Bitmap bitmap =BitmapSetting.decodeSampledBitmapFromFile(getTempImageFile(), 100, 100);
        // Bitmap bitmap =BitmapSetting.decodeSampledBitmapFromFile(getTempImageFile(), mImageSizeBoundary);

        // image boundary size 에 맞도록 이미지 축소.
        bitmap = resizeImageWithinBoundary(bitmap);

        // 결과 file 을 얻어갈 수 있는 메서드 제공.
        // saveBitmapToFile(bitmap);

        // show image on ImageView (저장한 파일 읽어와서 출력하는 듯)
        // Bitmap bm =
        // BitmapFactory.decodeFile(getTempImageFile().getAbsolutePath());
        // mImageView.setImageBitmap(bitmap);

        TourImage tourImage = new TourImage(path, bitmap);
        mImgList.add(tourImage);
    }

    /** 원하는 크기의 이미지로 options 설정. */
    private Bitmap loadImageWithSampleSize(File file) {
        BitmapFactory.Options options = new BitmapFactory.Options();
        options.inJustDecodeBounds = true;
        BitmapFactory.decodeFile(file.getAbsolutePath(), options);

        int width = options.outWidth;
        int height = options.outHeight;
        int longSide = Math.max(width, height);
        int sampleSize = 1;
        Log.d(TAG, "longSide : " + String.valueOf(longSide));

        if (longSide >= mImageSizeBoundary) {
            // sampleSize = longSide / mImageSizeBoundary;
            sampleSize *= 4;
        } else if (longSide >= mImageSizeBoundary / 2) {
            sampleSize *= 2;
        }

        Log.d(TAG, "sampleSize : " + String.valueOf(sampleSize));
        options.inJustDecodeBounds = false;
        options.inSampleSize = sampleSize;
        // options.inPurgeable = true; // deprecated, 이거 해도 메모리 해제 안된데요
        options.inDither = false;

        Bitmap bitmap = BitmapFactory.decodeFile(file.getAbsolutePath(), options);
        return bitmap;
    }

    /**
     * mImageSizeBoundary 크기로 이미지 크기 조정. mImageSizeBoundary 보다 작은 경우 resize하지
     * 않음.
     */
    private Bitmap resizeImageWithinBoundary(Bitmap bitmap) {
        int width = bitmap.getWidth();
        int height = bitmap.getHeight();

        if (width > height) {
            if (width > mImageSizeBoundary) {
                bitmap = resizeBitmapWithWidth(bitmap, mImageSizeBoundary);
            }
        } else {
            if (height > mImageSizeBoundary) {
                bitmap = resizeBitmapWithHeight(bitmap, mImageSizeBoundary);
            }
        }
        return bitmap;
    }

    private Bitmap resizeBitmapWithHeight(Bitmap source, int wantedHeight) {
        if (source == null)
            return null;

        int width = source.getWidth();
        int height = source.getHeight();

        float resizeFactor = wantedHeight * 1f / height;

        int targetWidth, targetHeight;
        targetWidth = (int) (width * resizeFactor);
        targetHeight = (int) (height * resizeFactor);

        Bitmap resizedBitmap = Bitmap.createScaledBitmap(source, targetWidth, targetHeight, true);

        return resizedBitmap;
    }

    private Bitmap resizeBitmapWithWidth(Bitmap source, int wantedWidth) {
        if (source == null)
            return null;

        int width = source.getWidth();
        int height = source.getHeight();

        float resizeFactor = wantedWidth * 1f / width;

        int targetWidth, targetHeight;
        targetWidth = (int) (width * resizeFactor);
        targetHeight = (int) (height * resizeFactor);

        Bitmap resizedBitmap = Bitmap.createScaledBitmap(source, targetWidth, targetHeight, true);

        return resizedBitmap;
    }

    public void setmImageSizeBoundary(int mImageSizeBoundary) {
        this.mImageSizeBoundary = mImageSizeBoundary;
    }

    /**
     * 불러온 이미지 저장하는 메서드 (tour list는 필요없음)
     *
     * @param bitmap
     */
    private void saveBitmapToFile(Bitmap bitmap) {
        File target = getTempImageFile();
        try {
            FileOutputStream fos = new FileOutputStream(target, false);
            bitmap.compress(Bitmap.CompressFormat.PNG, 100, fos); // 100 : max
            // quality
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}