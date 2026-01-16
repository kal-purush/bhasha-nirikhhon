package com.example.mdhvr.memegenerator;

import android.content.DialogInterface;
import android.content.Intent;
import android.graphics.Bitmap;
import android.graphics.drawable.BitmapDrawable;
import android.net.Uri;
import android.os.Bundle;
import android.os.Environment;
import android.support.v7.app.AlertDialog;
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
    private Bitmap imageBitmap;
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
        if(MainActivity.mutableBitmap!=null)
            imageView.setImageBitmap(MainActivity.mutableBitmap);
        else
            imageView.setImageResource(R.drawable.placeholder);
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        getMenuInflater().inflate(R.menu.image_menu,menu);
        return true;
    }

    @Override
    public boolean onPrepareOptionsMenu(Menu menu) {

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
                setImageView();
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
        if(path==null){
            saveImage();
        }
            final Intent shareIntent = new Intent(Intent.ACTION_SEND);
            shareIntent.setType("image/png");
            shareIntent.putExtra(Intent.EXTRA_STREAM, path);
            startActivity(Intent.createChooser(shareIntent, "Share meme using :"));
    }

    private void setImageView() {
        //TODO



    }

    private void drawOnImage() {

        Intent intent= new Intent(ImageActivity.this, PaintActivity.class);
        startActivity(intent);

    }

    Uri path;

    private void saveImage() {
        OutputStream output;
        // Find the SD Card path
        File filepath = Environment.getExternalStorageDirectory();

        // Create a new folder in SD Card
        File dir = new File(filepath.getAbsolutePath()
                + "/MemeGenerator/");
        dir.mkdir();

        // Create a name for the saved image
        String nameOfImage= "meme"+System.currentTimeMillis()+ ".png";
        File file = new File(dir, nameOfImage);
        path = Uri.parse(file.getAbsolutePath());

        try {
            output = new FileOutputStream(file);
            // Compress into image of png format
            MainActivity.mutableBitmap.compress(Bitmap.CompressFormat.PNG, 100, output);
            output.flush();
            output.close();
        }

        catch (Exception e) {
            e.printStackTrace();
        }

        Toast.makeText(getApplicationContext(),"Meme Saved",Toast.LENGTH_SHORT).show();
    }

    private void deleteImage() {
        showDeleteDialog();

    }

    @Override
    public void onBackPressed() {

        if(purpose.equals("EditImage")){
            showAlertDialog();
        }
       // super.onBackPressed();
    }

    private void showAlertDialog() {
        AlertDialog.Builder builder= new AlertDialog.Builder(ImageActivity.this);
        builder.setMessage("Changes may not be saved...");
        builder.setPositiveButton("Discard.", new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialogInterface, int i) {
                finish();
            }
        });
        builder.setNegativeButton("Keep Editing!", new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialogInterface, int i) {
                dialogInterface.dismiss();
            }
        });
        builder.create().show();
    }

    private void showDeleteDialog() {
        AlertDialog.Builder builder= new AlertDialog.Builder(ImageActivity.this);
        builder.setMessage("Are you sure you want to delete this meme ? Don't regret afterwards...");
        builder.setPositiveButton("I don't give a damn. Just delete it!", new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialogInterface, int i) {
                imageView.setImageResource(R.drawable.placeholder);
                Toast.makeText(ImageActivity.this,"Meme deleted",Toast.LENGTH_SHORT).show();
                MainActivity.mutableBitmap=((BitmapDrawable) imageView.getDrawable()).getBitmap();
                finish();
            }
        });
        builder.setNegativeButton("Thanks,it was by mistake!", new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialogInterface, int i) {
                dialogInterface.dismiss();
            }
        });
        builder.setTitle("*MemeIsGonnaBeDeleted*");
        builder.create().show();
    }

    private int myRequestCode=100;

    private void cropImage() {
        if(path==null){
            saveImage();
        }
    }

}