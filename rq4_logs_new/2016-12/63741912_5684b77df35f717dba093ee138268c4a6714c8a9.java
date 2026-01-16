package hise.hznu.istudy.util.clip;

import android.annotation.TargetApi;
import android.content.Intent;
import android.graphics.Bitmap;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.support.annotation.ColorInt;
import android.support.annotation.IdRes;
import android.support.annotation.NonNull;
import android.support.v7.app.ActionBar;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.Toolbar;
import android.text.TextUtils;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import com.yalantis.ucrop.util.BitmapLoadUtils;
import com.yalantis.ucrop.view.CropImageView;
import com.yalantis.ucrop.view.GestureCropImageView;
import com.yalantis.ucrop.view.TransformImageView;
import com.yalantis.ucrop.view.widget.AspectRatioTextView;
import com.yalantis.ucrop.view.widget.HorizontalProgressWheelView;

import java.io.OutputStream;

import hise.hznu.istudy.R;

/**
 * Created by Oleksii Shliama (https://github.com/shliama).
 */
public class CropActivity extends AppCompatActivity {

    public static final int DEFAULT_MAX_BITMAP_SIZE = 0;
    public static final int DEFAULT_COMPRESS_QUALITY = 90;
    public static final Bitmap.CompressFormat DEFAULT_COMPRESS_FORMAT = Bitmap.CompressFormat.JPEG;

    private static final String TAG = "UCropActivity";

    private static final int SCALE_WIDGET_SENSITIVITY_COEFFICIENT = 15000;
    private static final int ROTATE_WIDGET_SENSITIVITY_COEFFICIENT = 42;

    private MyGestureCropImageView mGestureCropImageView;

    private Uri mOutputUri;

    private int mMaxBitmapSize = DEFAULT_MAX_BITMAP_SIZE;
    private Bitmap.CompressFormat mCompressFormat = DEFAULT_COMPRESS_FORMAT;
    private int mCompressQuality = DEFAULT_COMPRESS_QUALITY;
    private boolean mGesturesAlwaysEnabled = false;

    private ImageView ivBack;
    private TextView tvSave;
    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_crop);
        setupViews();
        setImageData();
        setInitialState();
    }

    @Override
    protected void onStop() {
        super.onStop();
        if (mGestureCropImageView != null) {
            mGestureCropImageView.cancelAllAnimations();
        }
    }

    private void setImageData() {
        final Intent intent = getIntent();

        Uri inputUri = intent.getParcelableExtra(MCrop.EXTRA_INPUT_URI);
        mOutputUri = intent.getParcelableExtra(MCrop.EXTRA_OUTPUT_URI);
        processOptions(intent);

        if (inputUri != null && mOutputUri != null) {
            try {
                mGestureCropImageView.setMaxBitmapSize(mMaxBitmapSize);
                mGestureCropImageView.setImageUri(inputUri);
            } catch (Exception e) {
                setResultException(e);
                finish();
            }
        } else {
            setResultException(new NullPointerException(getString(R.string.ucrop_error_input_data_is_absent)));
            finish();
        }

        if (intent.getBooleanExtra(MCrop.EXTRA_ASPECT_RATIO_SET, false)) {

            int aspectRatioX = intent.getIntExtra(MCrop.EXTRA_ASPECT_RATIO_X, 0);
            int aspectRatioY = intent.getIntExtra(MCrop.EXTRA_ASPECT_RATIO_Y, 0);

            if (aspectRatioX > 0 && aspectRatioY > 0) {
                mGestureCropImageView.setTargetAspectRatio(aspectRatioX / (float) aspectRatioY);
            } else {
                mGestureCropImageView.setTargetAspectRatio(CropImageView.SOURCE_IMAGE_ASPECT_RATIO);
            }
        }

        if (intent.getBooleanExtra(MCrop.EXTRA_MAX_SIZE_SET, false)) {
            int maxSizeX = intent.getIntExtra(MCrop.EXTRA_MAX_SIZE_X, 0);
            int maxSizeY = intent.getIntExtra(MCrop.EXTRA_MAX_SIZE_Y, 0);

            if (maxSizeX > 0 && maxSizeY > 0) {
                mGestureCropImageView.setMaxResultImageSizeX(maxSizeX);
                mGestureCropImageView.setMaxResultImageSizeY(maxSizeY);
            } else {
                Log.w(TAG, "EXTRA_MAX_SIZE_X and EXTRA_MAX_SIZE_Y must be greater than 0");
            }
        }
    }

    private void processOptions(@NonNull Intent intent) {
        MCrop.Options options = intent.getParcelableExtra(MCrop.EXTRA_OPTIONS);
        if (options != null) {
            mMaxBitmapSize = options.getMaxBitmapSize();

            String compressionFormatName = options.getCompressionFormatName();
            Bitmap.CompressFormat compressFormat = null;
            if (!TextUtils.isEmpty(compressionFormatName)) {
                compressFormat = Bitmap.CompressFormat.valueOf(compressionFormatName);
            }
            mCompressFormat = (compressFormat == null) ? DEFAULT_COMPRESS_FORMAT : compressFormat;

            mCompressQuality = options.getCompressionQuality();
            mGesturesAlwaysEnabled = options.isGesturesAlwaysEnabled();
        }
    }

    private void setupViews() {
        ivBack = (ImageView)findViewById(R.id.iv_back);
        ivBack.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                finish();
            }
        });
        tvSave = (TextView)findViewById(R.id.tv_save);
        tvSave.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                cropAndSaveImage();
            }
        });
        mGestureCropImageView = (MyGestureCropImageView) findViewById(R.id.image_view_crop);
    }

    /**
     * Sets status-bar color for L devices.
     *
     * @param color - status-bar color
     */
    @TargetApi(Build.VERSION_CODES.LOLLIPOP)
    public void setStatusBarColor(@ColorInt int color) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.LOLLIPOP) {
            if (getWindow() != null) {
                getWindow().setStatusBarColor(color);
            }
        }
    }


    private final View.OnClickListener mStateClickListener = new View.OnClickListener() {
        @Override
        public void onClick(View v) {
            if (!v.isSelected()) {
                setWidgetState(v.getId());
            }
        }
    };

    private void setInitialState() {
        setWidgetState(R.id.state_scale);
    }

    private void setWidgetState(@IdRes int stateViewId) {
        mGestureCropImageView.setRotateEnabled(true);
        mGestureCropImageView.setScaleEnabled(true);
    }

    private void cropAndSaveImage() {
        OutputStream outputStream = null;
        try {
            final Bitmap croppedBitmap = mGestureCropImageView.cropImage();
            if (croppedBitmap != null) {
                outputStream = getContentResolver().openOutputStream(mOutputUri);
                croppedBitmap.compress(mCompressFormat, mCompressQuality, outputStream);
                croppedBitmap.recycle();
                Log.e("saveImage","" + mOutputUri.getPath());
                setResultUri(mOutputUri);
                finish();
            }
        } catch (Exception e) {
            Log.e("Exception", e.getMessage());
            setResultException(e);
            finish();

        } finally {
            BitmapLoadUtils.close(outputStream);
        }
    }

    private void setResultUri(Uri uri) {
        setResult(RESULT_OK, new Intent().putExtra(MCrop.EXTRA_OUTPUT_URI, uri));
    }

    private void setResultException(Throwable throwable) {
        setResult(MCrop.RESULT_ERROR, new Intent().putExtra(MCrop.EXTRA_ERROR, throwable));
    }

}