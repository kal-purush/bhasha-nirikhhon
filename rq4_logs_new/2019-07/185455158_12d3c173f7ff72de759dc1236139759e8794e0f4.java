package com.hotsauce.meem.TemplateCreator;

import android.content.Intent;
import android.graphics.Color;
import android.graphics.Rect;
import android.net.Uri;
import android.os.Bundle;
import android.os.Environment;
import android.util.Log;
import android.util.TypedValue;
import android.view.MotionEvent;
import android.view.View;
import android.view.ViewGroup;
import android.view.Window;
import android.view.WindowManager;
import android.widget.ImageButton;
import android.widget.ImageView;
import android.widget.RelativeLayout;
import android.widget.TextView;
import androidx.appcompat.app.AppCompatActivity;
import androidx.lifecycle.ViewModelProviders;
import com.hotsauce.meem.MemeViewModel;
import com.hotsauce.meem.PhotoEditor.MultiTouchListener;
import com.hotsauce.meem.PhotoEditor.TextEditorDialogFragment;
import com.hotsauce.meem.R;
import com.hotsauce.meem.db.MemeTemplate;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TemplateEditorActivity extends AppCompatActivity implements View.OnTouchListener, View.OnClickListener {

    List<Rect> rectangles;
    Uri imageUri;
    private MemeViewModel memeViewModel;
    private RelativeLayout imageLayout;
    private ImageView image;

    int REQ_LOAD_IMG = 1;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        // Calls image selector
        Intent photoPickerIntent = new Intent(Intent.ACTION_PICK);
        photoPickerIntent.setType("image/*");
        startActivityForResult(photoPickerIntent, REQ_LOAD_IMG);

        // Strip title and notification bars
        this.requestWindowFeature(Window.FEATURE_NO_TITLE);
        this.getWindow().setFlags(WindowManager.LayoutParams.FLAG_FULLSCREEN, WindowManager.LayoutParams.FLAG_FULLSCREEN);

        setContentView(R.layout.template_editor);

        imageLayout = (RelativeLayout)findViewById(R.id.imageLayout);
        image = (ImageView)findViewById(R.id.image);

        // Set button listeners
        ImageButton closeButton = (ImageButton)findViewById(R.id.closeButton);
        closeButton.setOnClickListener(this);
        ImageButton saveButton = (ImageButton)findViewById(R.id.saveButton);
        saveButton.setOnClickListener(this);
        ImageButton addTextButton = (ImageButton)findViewById(R.id.addTextButton);
        addTextButton.setOnClickListener(this);

        rectangles = new ArrayList<Rect>();
        rectangles.add(new Rect(300, 300, 600, 600));
        rectangles.add(new Rect(500, 1000, 800, 1300));

        createTextBox(300, 800, 300, 600);

        memeViewModel = ViewModelProviders.of(this).get(MemeViewModel.class);
    }

    public void createTextBox(int x1, int x2, int y1, int y2) {
        TextView tv = new TextView(this);
        ViewGroup.LayoutParams lp = new ViewGroup.LayoutParams(
                Math.abs(x1-x2),
                Math.abs(y1-y2));

        // size textView to fit width and height
        tv.setAutoSizeTextTypeUniformWithConfiguration(1, 100, 1, TypedValue.COMPLEX_UNIT_DIP);
        tv.setLayoutParams(lp);
        tv.setText("");

        tv.setBackgroundColor(Color.parseColor("#55FF0000"));
        MultiTouchListener multiTouchListener = new MultiTouchListener(null, image, true);

        imageLayout.addView(tv);

        final ViewGroup.MarginLayoutParams lpt = (ViewGroup.MarginLayoutParams)tv.getLayoutParams();
        Log .i("CreateTemplateActivityLog", "margins: " + x1 + "," + y1);
        lpt.setMargins(x1, y1, 0, 0);
        tv.setOnTouchListener(multiTouchListener);
    }

    public void saveTemplate() {
        String memeTemplateId = Long.toString(System.currentTimeMillis());
        saveTemplateImageFile(memeTemplateId);
        memeViewModel.insertTemplate(new MemeTemplate(memeTemplateId, rectangles));
    }

    void saveTemplateImageFile(String memeTemplateId)
    {
        String sourceFilename = imageUri.getPath();
        String destinationFilename = Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_PICTURES)
                + File.separator + memeTemplateId + ".png";

        BufferedInputStream bis = null;
        BufferedOutputStream bos = null;

        try {
            bis = new BufferedInputStream(new FileInputStream(sourceFilename));
            bos = new BufferedOutputStream(new FileOutputStream(destinationFilename, false));
            byte[] buf = new byte[1024];
            bis.read(buf);
            do {
                bos.write(buf);
            } while(bis.read(buf) != -1);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (bis != null) bis.close();
                if (bos != null) bos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean onTouch (View v, MotionEvent ev) {
        final int action = ev.getAction();
        switch (action) {
            case MotionEvent.ACTION_DOWN :
                // TODO
                Log .i("CreateTemplateActivityLog", "ACTION_DOWN");
                break;
            case MotionEvent.ACTION_UP :
                // TODO
                Log .i("CreateTemplateActivityLog", "x:" + ev.getX());
                Log .i("CreateTemplateActivityLog", "y:" + ev.getY());
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
                saveTemplate();
                onBackPressed();
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

    @Override
    protected void onActivityResult(int reqCode, int resultCode, Intent data) {
        super.onActivityResult(reqCode, resultCode, data);

        if (reqCode == REQ_LOAD_IMG) {
            if (resultCode == RESULT_OK) {
                final Uri imageUri = data.getData();

                image = findViewById(R.id.image);
                image.setImageURI(imageUri);
                image.setOnTouchListener(this);
            } else {
                onBackPressed();
            }
        }
    }
}