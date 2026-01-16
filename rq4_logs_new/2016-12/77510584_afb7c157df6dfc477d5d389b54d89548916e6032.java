package com.hankou.home.view;

import android.graphics.Bitmap;
import android.graphics.BitmapFactory;

import com.google.vr.sdk.widgets.pano.VrPanoramaView;
import com.hankou.R;
import com.hankou.base.BaseActivity;

import java.io.IOException;
import java.io.InputStream;

import butterknife.BindView;

/**
 * Created by bykj003 on 2016/12/28.
 */

public class VRTestActivity extends BaseActivity {

    @BindView(R.id.vrPanoramaView)
    public VrPanoramaView mVRView;

    @Override
    public int getLayoutResId() {
        return R.layout.act_vr_test;
    }

    @Override
    public void initViews() {
        VrPanoramaView.Options options = new VrPanoramaView.Options();
        options.inputType = VrPanoramaView.Options.TYPE_STEREO_OVER_UNDER;
        InputStream open = null;
        try {
            open = getAssets().open("andes.jpg");
        } catch (IOException e) {
            e.printStackTrace();
        }
        Bitmap bitmap = BitmapFactory.decodeStream(open);
        mVRView.loadImageFromBitmap(bitmap, options);
    }

    @Override
    public void showError(String message) {

    }
}