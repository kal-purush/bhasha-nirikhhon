package com.example.artisanprofilingapp;


import androidx.appcompat.app.AppCompatActivity;
import androidx.core.app.ActivityCompat;
import androidx.core.content.ContextCompat;

import android.Manifest;
import android.app.AlertDialog;
import android.content.DialogInterface;
import android.content.SharedPreferences;
import android.content.pm.PackageManager;
import android.graphics.Matrix;
import android.media.ExifInterface;
import android.media.MediaPlayer;
import android.os.Build;
import android.os.Bundle;

import android.content.Intent;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Environment;
import android.os.StrictMode;
import android.provider.MediaStore;
import android.provider.Settings;
import android.util.Base64;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.Toast;

import com.android.volley.AuthFailureError;
import com.android.volley.Request;
import com.android.volley.RequestQueue;
import com.android.volley.Response;
import com.android.volley.VolleyError;
import com.android.volley.toolbox.StringRequest;
import com.android.volley.toolbox.Volley;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ProfilePicActivity extends AppCompatActivity {
    private static final int PERMISSION_REQUEST_CODE = 100;
    private Button button;
    private String encoded_string, image_name;
    int count = 0;
    private Bitmap bitmap;
    private File file;
    private Uri file_uri;
    SharedPreferences myPref;
    private String dataToGet, idToGet;
    private String ImageCountToGet;
    private MediaPlayer mediaPlayer;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_profile_pic);

        button = (Button) findViewById(R.id.start);
        myPref = getApplicationContext().getSharedPreferences("MyPref",MODE_PRIVATE);
        dataToGet = myPref.getString("phone","No data found");
        idToGet = myPref.getString("id","No data found");
        ImageCountToGet = myPref.getString("count","No data found");
        mediaPlayer = MediaPlayer.create(this, R.raw.profilepicinst);
        String state = Environment.getExternalStorageState();
        if (Environment.MEDIA_MOUNTED.equals(state)) {
            if (Build.VERSION.SDK_INT >= 23) {
                if (!checkPermission()) {

                    requestPermission();
                }
            }
        }
        mediaPlayer.start();

        StrictMode.VmPolicy.Builder builder = new StrictMode.VmPolicy.Builder();
        StrictMode.setVmPolicy(builder.build());
        button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {

                            mediaPlayer.start();

                    getFileUri();

                count = Integer.parseInt(ImageCountToGet) +1;
                Log.d("count",String.valueOf(count));
                ImageCountToGet = String.valueOf(count);
                myPref.edit().putString("count", ImageCountToGet).apply();
                Intent i = new Intent(MediaStore.ACTION_IMAGE_CAPTURE);
                i.putExtra(MediaStore.EXTRA_OUTPUT, file_uri);
                startActivityForResult(i, 10);
            }
        });
    }

    private void getFileUri() {
        //img_type = "yes";
        image_name = "_"+ dataToGet + ".jpg";
        //img_type = ".jpg";
        Log.d("msg","asche ei kane?");
        file = new File(Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_PICTURES)
                + File.separator + count + image_name
        );
        Log.d("msg" , String.valueOf(file));
        file_uri = Uri.fromFile(file);
        Log.d("msg" , String.valueOf(file_uri));
    }
    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {

        super.onActivityResult(requestCode, resultCode, data);
        if (requestCode == 10 && resultCode == RESULT_OK) {
            new ProfilePicActivity.Encode_image().execute();
        }
    }

    public static Bitmap rotateImage(Bitmap source, float angle) {
        Matrix matrix = new Matrix();
        matrix.postRotate(angle);
        return Bitmap.createBitmap(source, 0, 0, source.getWidth(), source.getHeight(),
                matrix, true);
    }

    private class Encode_image extends AsyncTask<Void, Void, Void> {
        @Override
        protected Void doInBackground(Void... voids) {
            bitmap = BitmapFactory.decodeFile(file_uri.getPath());
            ExifInterface ei = null;
            try {
                ei = new ExifInterface(Objects.requireNonNull(file_uri.getPath()));
            } catch (IOException e) {
                e.printStackTrace();
            }
            assert ei != null;
            int orientation = ei.getAttributeInt(ExifInterface.TAG_ORIENTATION,
                    ExifInterface.ORIENTATION_UNDEFINED);

            Bitmap rotatedBitmap = null;
            switch(orientation) {

                case ExifInterface.ORIENTATION_ROTATE_90:
                    rotatedBitmap = rotateImage(bitmap, 90);
                    break;

                case ExifInterface.ORIENTATION_ROTATE_180:
                    rotatedBitmap = rotateImage(bitmap, 180);
                    break;

                case ExifInterface.ORIENTATION_ROTATE_270:
                    rotatedBitmap = rotateImage(bitmap, 270);
                    break;

                case ExifInterface.ORIENTATION_NORMAL:
                default:
                    rotatedBitmap = bitmap;
            }
            bitmap = BitmapFactory.decodeFile(file_uri.getPath());
//            Log.d("hi", "doInBackground: "+bitmap.toString());
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            bitmap.compress(Bitmap.CompressFormat.JPEG, 50, stream);//compress image as per ur need
            bitmap.recycle();

            byte[] array = stream.toByteArray();
            encoded_string = Base64.encodeToString(array, 0);
            return null;
        }
        @Override
        protected void onPostExecute(Void aVoid) {
            makeRequest();
            Toast.makeText(ProfilePicActivity.this, "picture submitted successfully!", Toast.LENGTH_LONG).show();

            myPref.edit().putString("track", "7").apply();
            Intent i=new Intent(ProfilePicActivity.this,ArtformActivity.class);
            startActivity(i);
        }
    }

    private void makeRequest() {
        RequestQueue requestQueue = Volley.newRequestQueue(this);

        StringRequest request = new StringRequest(Request.Method.POST, "http://192.168.43.12/Artisans-Profiling/profilepic.php",
                new Response.Listener<String>() {
                    @Override
                    public void onResponse(String response) {

                    }
                }, new Response.ErrorListener() {
            @Override
            public void onErrorResponse(VolleyError error) {

            }
        }) {
            @Override
            protected Map<String, String> getParams() throws AuthFailureError {
                HashMap<String,String> map = new HashMap<>();
                map.put("encoded_string",encoded_string);
                map.put("image_name",count+image_name);
                map.put("id",idToGet);

                return map;
            }
        };
        requestQueue.add(request);
    }

    private boolean checkPermission() {
        int result = ContextCompat.checkSelfPermission(ProfilePicActivity.this, Manifest.permission.WRITE_EXTERNAL_STORAGE);
        //int result2 = ContextCompat.checkSelfPermission(Insert_image_instructionActivity.this, Manifest.permission.CAMERA);
        if (result == PackageManager.PERMISSION_GRANTED ) {
            return true;
        } else {
            return false;
        }
    }

//    private boolean checkPermission() {
//        // int result = ContextCompat.checkSelfPermission(CaptureImageActivity.this, android.Manifest.permission.WRITE_EXTERNAL_STORAGE);
//        int result2 = ContextCompat.checkSelfPermission(ProfilePicActivity.this, Manifest.permission.CAMERA);
//        if (/*result == PackageManager.PERMISSION_GRANTED && */result2 == PackageManager.PERMISSION_GRANTED) {
//            return true;
//        } else {
//            return false;
//        }
//    }
//
//    private void requestPermission() {
//        int i=0;
////        if (ActivityCompat.shouldShowRequestPermissionRationale(CaptureImageActivity.this, android.Manifest.permission.WRITE_EXTERNAL_STORAGE)) {
////           i=1;
////            Toast.makeText(CaptureImageActivity.this, "Write External Storage permission allows us to save files. Please allow this permission in App Settings.", Toast.LENGTH_LONG).show();
////        }
//        if(ActivityCompat.shouldShowRequestPermissionRationale(ProfilePicActivity.this, Manifest.permission.WRITE_EXTERNAL_STORAGE)) {
//            i=2;
//            Toast.makeText(ProfilePicActivity.this, "Camera permission allows us to Click Pictures. Please allow this permission in App Settings.", Toast.LENGTH_LONG).show();
//        }
//
//
//        else {
//            i=3;
//            //ActivityCompat.requestPermissions(CaptureImageActivity.this, new String[]{android.Manifest.permission.WRITE_EXTERNAL_STORAGE}, PERMISSION_REQUEST_CODE);
//            ActivityCompat.requestPermissions(ProfilePicActivity.this, new String[]{Manifest.permission.WRITE_EXTERNAL_STORAGE}, PERMISSION_REQUEST_CODE);
//        }
//        Log.d("check",Integer.toString(i));
//    }
//
//    @Override
//    public void onRequestPermissionsResult(int requestCode, String permissions[], int[] grantResults) {
//        switch (requestCode) {
//            case PERMISSION_REQUEST_CODE:
//                if (grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
//                    Log.e("value", "Permission Granted, Now you can use local drive .");
//                } else {
//                    Log.e("value", "Permission Denied, You cannot use local drive .");
//                }
//                break;
//        }
//    }
private void requestPermission() {
    int i=0;
//        if (ActivityCompat.shouldShowRequestPermissionRationale(this, Manifest.permission.WRITE_EXTERNAL_STORAGE)) {
//            i=1;
//            Toast.makeText(this, "Camera permission allows us to click images. Please allow this permission in App Settings.", Toast.LENGTH_LONG).show();
//        }
//
//
//
//        else {
//            i=3;
//            ActivityCompat.requestPermissions(this, new String[]{Manifest.permission.WRITE_EXTERNAL_STORAGE}, PERMISSION_REQUEST_CODE);
//
//        }

    if(ActivityCompat.shouldShowRequestPermissionRationale(this, Manifest.permission.WRITE_EXTERNAL_STORAGE))
    {
        new AlertDialog.Builder(this)
                .setTitle("Permission Needed")
                .setMessage("Permission is needed to access files from your device...")
                .setPositiveButton("OK", new DialogInterface.OnClickListener() {
                    @Override
                    public void onClick(DialogInterface dialog, int which) {
                        ActivityCompat.requestPermissions(ProfilePicActivity.this,new String[]{Manifest.permission.WRITE_EXTERNAL_STORAGE},666);
                    }
                })
                .setNegativeButton("Cancel", new DialogInterface.OnClickListener() {
                    @Override
                    public void onClick(DialogInterface dialog, int which) {
                        dialog.dismiss();
                    }
                }).create().show();
    }
    else
    {
        ActivityCompat.requestPermissions(this,new String[]{Manifest.permission.WRITE_EXTERNAL_STORAGE},666);
    }
    Log.d("check",Integer.toString(i));
}



    @Override
    public void onRequestPermissionsResult(int requestCode, String permissions[], int[] grantResults) {
//        switch (requestCode) {
//            case PERMISSION_REQUEST_CODE:
//                if (grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
//                    myPref.edit().putString("track", "6").apply();
//                    Log.e("value", "Permission Granted, Now you can use local drive .");
//                    Intent i = new Intent(Insert_image_instructionActivity.this, CaptureImageActivity.class);
//                    startActivity(i);
//                } else {
//                    Log.e("value", "Permission Denied, You cannot use local drive .");
//                }
//                break;
////            case 9:
////                if (grantResults.length > 1 && grantResults[1] == PackageManager.PERMISSION_GRANTED) {
////                    Log.e("value", "Permission Granted, Now you can use camera .");
////                } else {
////                    Log.e("value", "Permission Denied, You cannot use camera.");
////                }
////                break;
//        }
        switch (requestCode) {

            case 666: // Allowed was selected so Permission granted
                if (grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {

                    Log.d("Jhingalala", "granted");
                    mediaPlayer.start();

                    // do your work here

                } else if (Build.VERSION.SDK_INT >= 23 && !shouldShowRequestPermissionRationale(permissions[0])) {
                    // User selected the Never Ask Again Option Change settings in app settings manually
                    AlertDialog.Builder alertDialogBuilder = new AlertDialog.Builder(this);
                    alertDialogBuilder.setTitle("Change Permissions in Settings");
                    alertDialogBuilder
                            .setMessage("" +
                                    "\nClick SETTINGS to Manually Set\n" + "Permissions to use Database Storage")
                            .setCancelable(false)
                            .setPositiveButton("SETTINGS", new DialogInterface.OnClickListener() {
                                public void onClick(DialogInterface dialog, int id) {
                                    Intent intent = new Intent(Settings.ACTION_APPLICATION_DETAILS_SETTINGS);
                                    Uri uri = Uri.fromParts("package", getPackageName(), null);
                                    intent.setData(uri);
                                    startActivityForResult(intent, 1000);     // Comment 3.
                                }
                            });

                    AlertDialog alertDialog = alertDialogBuilder.create();
                    alertDialog.show();

                } else {
                    // User selected Deny Dialog to EXIT App ==> OR <== RETRY to have a second chance to Allow Permissions
                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M && checkSelfPermission(Manifest.permission.WRITE_EXTERNAL_STORAGE) == PackageManager.PERMISSION_DENIED) {

                        AlertDialog.Builder alertDialogBuilder = new AlertDialog.Builder(this);
                        alertDialogBuilder.setTitle("Second Chance");
                        alertDialogBuilder
                                .setMessage("Click RETRY to Set Permissions to Allow\n\n" + "Click EXIT to the Close App")
                                .setCancelable(false)
                                .setPositiveButton("RETRY", new DialogInterface.OnClickListener() {
                                    public void onClick(DialogInterface dialog, int id) {
                                        //ActivityCompat.requestPermissions((Activity) context, new String[]{Manifest.permission.READ_EXTERNAL_STORAGE}, Integer.parseInt(WRITE_EXTERNAL_STORAGE));
                                        Intent i = new Intent(ProfilePicActivity.this, Insert_image_instructionActivity.class);
                                        i.addFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP | Intent.FLAG_ACTIVITY_NEW_TASK);
                                        startActivity(i);
                                    }
                                })
                                .setNegativeButton("EXIT", new DialogInterface.OnClickListener() {
                                    public void onClick(DialogInterface dialog, int id) {
                                        dialog.cancel();
                                        finishAffinity();
                                        System.exit(0);
                                    }
                                });
                        AlertDialog alertDialog = alertDialogBuilder.create();
                        alertDialog.show();
                    }
                }
                break;
        }
    }

}