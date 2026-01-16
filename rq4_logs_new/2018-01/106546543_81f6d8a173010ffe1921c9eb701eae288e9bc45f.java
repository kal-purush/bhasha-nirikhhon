package com.example.yukai.mydefinedview.ArtLearningNote.chapter6_1;

import android.app.Activity;
import android.graphics.drawable.ClipDrawable;
import android.graphics.drawable.ColorDrawable;
import android.graphics.drawable.Drawable;
import android.graphics.drawable.ScaleDrawable;
import android.graphics.drawable.TransitionDrawable;
import android.os.Bundle;
import android.support.annotation.Nullable;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageView;
import android.widget.SeekBar;

import com.example.yukai.mydefinedview.R;

/**
 * Created by yukai on 2018/1/30.
 * xml drawable test
 * layer-list
 * level-list
 * transitionDrawable
 * insetDrawable
 * scaleDrawable
 * clipDrawable
 */

public class Chapter6_1 extends Activity implements View.OnClickListener{

    private ImageView mLevelImage;
    private Button mLevelImageButton;
    private EditText mLevelListEditText;
    private ImageView mTransitionDrawableTest;
    private ImageView mScaleDrawableImage;
    private ImageView mClipDrawableImage;
    private SeekBar mSeekBar;

    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.chapter6_1_layout);

        //level list drawable
        mLevelImage = (ImageView) findViewById(R.id.level_list_image);
        mLevelListEditText = (EditText) findViewById(R.id.level_list_edit_text);
        mLevelImageButton = (Button) findViewById(R.id.change_level_btn);
        mLevelImageButton.setOnClickListener(this);

        //transiton drawable
        mTransitionDrawableTest = (ImageView) findViewById(R.id.transition_drawable_image);
        final TransitionDrawable transitionDrawable = new TransitionDrawable(new Drawable[] { new ColorDrawable(0xfffcfcfc),
                getResources().getDrawable(R.drawable.hilton)});
        mTransitionDrawableTest.setImageDrawable(transitionDrawable);
        transitionDrawable.startTransition(1000);

        //scale drawable
        mScaleDrawableImage = (ImageView) findViewById(R.id.scale_drawable_image);
        ScaleDrawable scaleDrawable = (ScaleDrawable) mScaleDrawableImage.getDrawable();
        scaleDrawable.setLevel(1);

        //clip drawable
        mSeekBar = (SeekBar) findViewById(R.id.seek_bar);
        mClipDrawableImage = (ImageView) findViewById(R.id.clip_drawable_image);
        mSeekBar.setOnSeekBarChangeListener(new SeekBar.OnSeekBarChangeListener() {
            @Override
            public void onProgressChanged(SeekBar seekBar, int progress, boolean fromUser) {
                int max = seekBar.getMax();
                double scale = (double)progress/(double)max;
                ClipDrawable drawable = (ClipDrawable) mClipDrawableImage.getBackground();
                drawable.setLevel((int) (10000*scale));
            }

            @Override
            public void onStartTrackingTouch(SeekBar seekBar) {

            }

            @Override
            public void onStopTrackingTouch(SeekBar seekBar) {

            }
        });
    }

    @Override
    public void onClick(View v) {
        int id = v.getId();
        if (id == R.id.change_level_btn){
            String level = mLevelListEditText.getText().toString();
            mLevelImage.setImageLevel(Integer.parseInt(level));
        }
    }
}