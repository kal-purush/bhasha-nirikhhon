package com.example.Smile;
import android.content.Intent;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.PointF;
import android.graphics.RectF;
import android.graphics.drawable.BitmapDrawable;
import android.os.Bundle;
import android.os.Handler;
import android.provider.MediaStore;
import android.support.v7.app.AppCompatActivity;
import android.util.SparseArray;
import android.view.MenuItem;
import android.view.View;
import android.widget.ImageView;
import android.widget.Toast;
import com.github.clans.fab.FloatingActionButton;
import com.github.clans.fab.FloatingActionMenu;
import com.google.android.gms.vision.Frame;
import com.google.android.gms.vision.face.Face;
import com.google.android.gms.vision.face.FaceDetector;
import com.google.android.gms.vision.face.Landmark;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class MainActivity extends AppCompatActivity {

    private FloatingActionButton fab1;
    private FloatingActionButton fab2;
    private FloatingActionButton fab3;
    private FloatingActionButton fab4;

    private static final int RQS_LOADIMAGE = 0;
    public static final String FILE_NAME = "temp.jpg";
    public static final int CAMERA_IMAGE_REQUEST = 3;
    private ImageView imageView;
    private Bitmap myBitmap;
    int value = 0;
    public static final int CAMERA_PERMISSIONS_REQUEST = 2;
    private List<FloatingActionMenu> menus = new ArrayList<>();
    private Handler mUiHandler = new Handler();
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        fab1 = (FloatingActionButton)findViewById(R.id.fab1);
        fab4 = (FloatingActionButton)findViewById(R.id.fab4);
        fab3 = (FloatingActionButton)findViewById(R.id.fab3);
        fab2 = (FloatingActionButton)findViewById(R.id.fab2);
        imageView = (ImageView)findViewById(R.id.imageView);


        getSupportActionBar().setDisplayHomeAsUpEnabled(true);
        FloatingActionMenu menuLabelsRight = (FloatingActionMenu) findViewById(R.id.menu_labels_right);
        menus.add(menuLabelsRight);
        menuLabelsRight.hideMenuButton(false);

        int delay = 400;
        for (final FloatingActionMenu menu : menus) {
            mUiHandler.postDelayed(new Runnable() {
                @Override
                public void run() {
                    menu.showMenuButton(true);
                }
            }, delay);
            delay += 150;
        }



        fab1.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent();
                intent.setType("image/*");
                intent.setAction(Intent.ACTION_GET_CONTENT);
                intent.addCategory(Intent.CATEGORY_OPENABLE);
                startActivityForResult(intent, RQS_LOADIMAGE);
            }
        });

        fab4.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                rt1(v);
            }
        });

        fab2.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                if (myBitmap == null) {
                    Toast.makeText(MainActivity.this,
                            "myBitmap == null",
                            Toast.LENGTH_LONG).show();
                } else {
                    detectFace();

                }
            }
        });
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        switch (item.getItemId()) {
            case android.R.id.home:
                finish();
                return true;
        }
        return super.onOptionsItemSelected(item);
    }

    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        if (requestCode == RQS_LOADIMAGE
                && resultCode == RESULT_OK){

            if(myBitmap != null){
                myBitmap.recycle();
            }

            try {
                InputStream inputStream =
                        getContentResolver().openInputStream(data.getData());
                myBitmap = BitmapFactory.decodeStream(inputStream);
                inputStream.close();

                 imageView.setImageBitmap(myBitmap);

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        super.onActivityResult(requestCode, resultCode, data);

    }

    private void detectFace(){


        Paint myRectPaint = new Paint();
        myRectPaint.setStrokeWidth(5);
        myRectPaint.setColor(Color.GREEN);
        myRectPaint.setStyle(Paint.Style.STROKE);

        Paint landmarksPaint = new Paint();
        landmarksPaint.setStrokeWidth(10);
        landmarksPaint.setColor(Color.RED);
        landmarksPaint.setStyle(Paint.Style.STROKE);

        Paint smilingPaint = new Paint();
        smilingPaint.setStrokeWidth(4);
        smilingPaint.setColor(Color.YELLOW);
        smilingPaint.setStyle(Paint.Style.STROKE);

        boolean somebodySmiling = false;
        boolean somebodyCrying = false;

        Bitmap tempBitmap = Bitmap.createBitmap(myBitmap.getWidth(), myBitmap.getHeight(), Bitmap.Config.RGB_565);
        Canvas tempCanvas = new Canvas(tempBitmap);
        tempCanvas.drawBitmap(myBitmap, 0, 0, null);



        FaceDetector faceDetector =
                new FaceDetector.Builder(getApplicationContext())
                        .setTrackingEnabled(false)
                        .setLandmarkType(FaceDetector.ALL_LANDMARKS)
                        .setClassificationType(FaceDetector.ALL_CLASSIFICATIONS)
                        .build();

        Frame frame = new Frame.Builder().setBitmap(myBitmap).build();
        SparseArray<Face> faces = faceDetector.detect(frame);

        for(int i=0; i<faces.size(); i++) {
            Face thisFace = faces.valueAt(i);
            float x1 = thisFace.getPosition().x;
            float y1 = thisFace.getPosition().y;
            float x2 = x1 + thisFace.getWidth();
            float y2 = y1 + thisFace.getHeight();
            tempCanvas.drawRoundRect(new RectF(x1, y1, x2, y2), 2, 2, myRectPaint);

            List<Landmark> landmarks = thisFace.getLandmarks();
            for(int l=0; l<landmarks.size(); l++){
                PointF pos = landmarks.get(l).getPosition();
                tempCanvas.drawPoint(pos.x, pos.y, landmarksPaint);
            }

            final float smilingAcceptProbability = 0.5f;
            float smilingProbability = thisFace.getIsSmilingProbability();
            Toast.makeText(MainActivity.this,
                    smilingProbability+"",
                    Toast.LENGTH_LONG).show();
            if(smilingProbability > smilingAcceptProbability){
                tempCanvas.drawOval(new RectF(x1, y1, x2, y2), smilingPaint);
                somebodySmiling = true;
            }
            if(smilingProbability < smilingAcceptProbability && smilingProbability> 0.15f){
                tempCanvas.drawOval(new RectF(x1, y1, x2, y2), smilingPaint);
                somebodyCrying = true;
            }

        }

        imageView.setImageDrawable(new BitmapDrawable(getResources(), tempBitmap));

        if(somebodySmiling){
            Toast.makeText(MainActivity.this,
                    "Smiling",
                    Toast.LENGTH_LONG).show();
        }
        else if(somebodyCrying){
            Toast.makeText(MainActivity.this,
                    "Crying",
                    Toast.LENGTH_LONG).show();
        }
        else{
            Toast.makeText(MainActivity.this,
                    "NotSmiling",
                    Toast.LENGTH_LONG).show();
        }

    }
   public void startcamera(View view) {
      Intent callcamintent = new Intent();
       callcamintent.setAction(MediaStore.ACTION_IMAGE_CAPTURE);
      startActivityForResult(callcamintent, RQS_LOADIMAGE);

  }

  public void rt1(View view) {
    value = value + 90;
        imageView.setRotation(value);
    }

    }
