package com.example.fruitseason;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;

import android.graphics.Bitmap;
import android.net.Uri;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.ImageView;
import android.widget.Toast;

import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.tasks.OnFailureListener;
import com.google.android.gms.tasks.OnSuccessListener;
import com.google.android.gms.tasks.Task;
import com.google.firebase.storage.FirebaseStorage;
import com.google.firebase.storage.StorageReference;
import com.google.firebase.storage.UploadTask;

import java.io.ByteArrayOutputStream;
import java.util.UUID;

public class ManagePictures extends AppCompatActivity {

    private ImageView imagePic;
    private Button buttonUpload;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_manage_pictures);

        buttonUpload = findViewById(R.id.buttonUpload);
        imagePic = findViewById(R.id.imageFruit);

        buttonUpload.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                imagePic.setDrawingCacheEnabled(true);
                imagePic.buildDrawingCache();

                Bitmap bitmap = imagePic.getDrawingCache();

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                bitmap.compress(Bitmap.CompressFormat.JPEG, 80, baos );

                byte[] imageData = baos.toByteArray();

                StorageReference storageReference = FirebaseStorage.getInstance().getReference();
                StorageReference images = storageReference.child("fruitsImages");

                String fileName = UUID.randomUUID().toString();
                StorageReference imageRef = images.child( fileName + ".jpeg");

                UploadTask uploadTask = imageRef.putBytes( imageData );

                uploadTask.addOnFailureListener(ManagePictures.this, new OnFailureListener() {
                    @Override
                    public void onFailure(@NonNull Exception e) {
                        Toast.makeText(ManagePictures.this,"Image upload failed: " + e.getMessage().toString(), Toast.LENGTH_LONG ).show();
                    }
                }).addOnSuccessListener(ManagePictures.this, new OnSuccessListener<UploadTask.TaskSnapshot>() {
                    @Override
                    public void onSuccess(UploadTask.TaskSnapshot taskSnapshot) {
                        imageRef.getDownloadUrl().addOnCompleteListener(new OnCompleteListener<Uri>() {
                            @Override
                            public void onComplete(@NonNull Task<Uri> task) {
                                Uri url = task.getResult();
                                Toast.makeText(ManagePictures.this,
                                        "Upload success: " + url.toString() ,
                                        Toast.LENGTH_LONG ).show();
                            }
                        });

                    }
                });

            }
        });
    }

}