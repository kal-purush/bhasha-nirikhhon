package com.hotsauce.meem;

import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.util.Log;
import android.view.MotionEvent;
import android.view.View;
import android.view.Window;
import android.view.WindowManager;
import android.widget.ImageButton;
import android.widget.ImageView;
import androidx.appcompat.app.AppCompatActivity;
import com.hotsauce.meem.PhotoEditor.TextEditorDialogFragment;
import ja.burhanrashid52.photoeditor.TextStyleBuilder;

public class CreateMemeActivity extends AppCompatActivity implements View.OnTouchListener, View.OnClickListener {
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        //Remove title bar
        this.requestWindowFeature(Window.FEATURE_NO_TITLE);
        //Remove notification bar
        this.getWindow().setFlags(WindowManager.LayoutParams.FLAG_FULLSCREEN, WindowManager.LayoutParams.FLAG_FULLSCREEN);

        setContentView(R.layout.activity_create_meme);

        Intent intent = getIntent();
        Uri imageUri = Uri.parse(intent.getStringExtra("imageUri"));
        ImageView image = findViewById(R.id.image);
        image.setImageURI(imageUri);
        image.setOnTouchListener(this);

        // Set button listeners
        ImageButton closeButton = (ImageButton)findViewById(R.id.closeButton);
        closeButton.setOnClickListener(this);
        ImageButton saveButton = (ImageButton)findViewById(R.id.saveButton);
        saveButton.setOnClickListener(this);
        ImageButton addTextButton = (ImageButton)findViewById(R.id.addTextButton);
        addTextButton.setOnClickListener(this);
    }

    public boolean onTouch (View v, MotionEvent ev) {
        final int action = ev.getAction();
        final int evX = (int) ev.getX();
        final int evY = (int) ev.getY();
        switch (action) {
            case MotionEvent.ACTION_DOWN :
                // TODO
                Log .i("CreateTemplateActivityLog", "ACTION_DOWN");
                break;
            case MotionEvent.ACTION_UP :
                // TODO
                Log .i("CreateTemplateActivityLog", "x:" + ev.getX());
                Log .i("CreateTemplateActivityLog", "y:" + ev.getY());
                if ( "coordinate in text region" ) {
                    TextEditorDialogFragment textEditorDialogFragment = TextEditorDialogFragment.show(this);
                    textEditorDialogFragment.setOnTextEditorListener(new TextEditorDialogFragment.TextEditor() {
                        @Override
                        public void onDone(String inputText, int colorCode) {
                            // TODO
                            Log .i("CreateTemplateActivityLog", "inputText:" + inputText);
                        }
                    });
                }
                break;
        }
        return true;
    }

    @Override
    public void onClick(View view) {
        switch (view.getId()) {
            case R.id.closeButton:
                onBackPressed();
                break;
            case R.id.saveButton:
                // TODO
                break;
            case R.id.addTextButton:
                // TODO
                TextEditorDialogFragment textEditorDialogFragment = TextEditorDialogFragment.show(this);
                textEditorDialogFragment.setOnTextEditorListener(new TextEditorDialogFragment.TextEditor() {
                    @Override
                    public void onDone(String inputText, int colorCode) {
                        // TODO
                        Log .i("CreateTemplateActivityLog", "inputText:" + inputText);
                    }
                });
                break;
        }
    }

    @Override
    public void onBackPressed() {
        // show save dialogue if user is in editing mode
        super.onBackPressed();
    }
}