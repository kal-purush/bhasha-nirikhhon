package com.example.mdhvr.memegenerator;

import android.content.Intent;
import android.graphics.Bitmap;
import android.net.Uri;
import android.os.Bundle;
import android.os.Environment;
import android.support.v7.app.AppCompatActivity;
import android.view.Menu;
import android.view.MenuItem;
import android.widget.ImageView;
import android.widget.Toast;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

public class ImageActivity extends AppCompatActivity {

    private String purpose;
    private ImageView imageView;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_image);
        Intent intent= getIntent();
        purpose= intent.getStringExtra("purpose");
        if(purpose.equals("PreviewImage")){
            setTitle("Preview");
            invalidateOptionsMenu();
        }
        else{
            setTitle("Edit");
            invalidateOptionsMenu();
        }

        imageView= findViewById(R.id.image_activity_image);
        imageView.setImageBitmap(MainActivity.mutableBitmap);

    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        getMenuInflater().inflate(R.menu.image_menu,menu);
        return true;
    }

    @Override
    public boolean onPrepareOptionsMenu(Menu menu) {

        //return super.onPrepareOptionsMenu(menu);
        if(purpose.equals("PreviewImage")){
            MenuItem m1= menu.findItem(R.id.crop);
            m1.setVisible(false);
            MenuItem m2= menu.findItem(R.id.draw);
            m2.setVisible(false);
            MenuItem m3 = menu.findItem(R.id.share);
            m3.setVisible(true);

        }
        if(purpose.equals("EditImage")){
            MenuItem m1= menu.findItem(R.id.crop);
            m1.setVisible(true);
            MenuItem m2= menu.findItem(R.id.draw);
            m2.setVisible(true);
            MenuItem m3 = menu.findItem(R.id.share);
            m3.setVisible(false);
        }
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {

        switch(item.getItemId()){
            case R.id.crop:
                cropImage();
                return true;
            case R.id.delete:
                deleteImage();
                return true;
            case R.id.save:
                if(purpose.equals("PreviewImage")) {
                    saveImage();

                }
                else
                setImageInMainActivity();
                return true;
            case R.id.draw:
                drawOnImage();
                return true;
            case R.id.share:
                shareImage();
                return true;

        }

        return super.onOptionsItemSelected(item);
    }

    private void shareImage() {

        final Intent shareIntent = new Intent(Intent.ACTION_SEND);
        shareIntent.setType("image/png");
        shareIntent.putExtra(Intent.EXTRA_STREAM, path);
        startActivity(Intent.createChooser(shareIntent,"Share meme using :"));
    }

    private void setImageInMainActivity() {

    }

    private void drawOnImage() {

    }

    Uri path;
    String nameOfImage;
    private void saveImage() {
        OutputStream output;
        // Find the SD Card path
        File filepath = Environment.getExternalStorageDirectory();

        // Create a new folder in SD Card
        File dir = new File(filepath.getAbsolutePath()
                + "/MemeGenerator/");
        dir.mkdir();

        // Create a name for the saved image
        nameOfImage= "meme"+System.currentTimeMillis()+ ".png";
        File file = new File(dir, "meme"+System.currentTimeMillis()+ ".png");
        path = Uri.parse(file.getAbsolutePath());

        try {

            output = new FileOutputStream(file);

            // Compress into png format image from 0% - 100
            MainActivity.mutableBitmap.compress(Bitmap.CompressFormat.PNG, 100, output);
            output.flush();
            output.close();
        }

        catch (Exception e) {
            e.printStackTrace();
        }

        Toast.makeText(getApplicationContext(),"Image Saved",Toast.LENGTH_SHORT).show();
    }

    private void deleteImage() {

    }

    private void cropImage() {

    }
}