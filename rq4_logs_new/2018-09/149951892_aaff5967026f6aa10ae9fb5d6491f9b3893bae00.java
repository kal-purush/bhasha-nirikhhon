package com.cglimpse.levankhang.cglimpse;

import android.content.Intent;
import android.net.Uri;
import android.support.annotation.NonNull;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.support.v7.widget.Toolbar;
import android.text.TextUtils;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageView;
import android.widget.ProgressBar;
import android.widget.Toast;

import com.bumptech.glide.Glide;
import com.google.android.gms.tasks.Continuation;
import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.tasks.Task;
import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.auth.FirebaseUser;
import com.google.firebase.firestore.DocumentSnapshot;
import com.google.firebase.firestore.FirebaseFirestore;
import com.google.firebase.firestore.SetOptions;
import com.google.firebase.storage.FirebaseStorage;
import com.google.firebase.storage.StorageReference;
import com.google.firebase.storage.UploadTask;
import com.theartofdev.edmodo.cropper.CropImage;
import com.theartofdev.edmodo.cropper.CropImageView;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import de.hdodenhof.circleimageview.CircleImageView;

public class SetupActivity extends AppCompatActivity {

    private Toolbar toolbar;
    private CircleImageView imageViewProfileImage;
    private ImageView imageViewEditProfileImage;
    private Button btnSave;
    private EditText editTextName;
    private ProgressBar progressBar;

//
    private Uri image;
//    final int GALLERY_CODE = 2;

//    Firebase
    private FirebaseAuth mAuth;
    private StorageReference storageReference;
    private FirebaseFirestore db;

    private String uid;
    private boolean isImageChange;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_setup);
        connectView();
        setupToolbar();
        addEvent();

        image = null;
        isImageChange = false;

        mAuth = FirebaseAuth.getInstance();
        storageReference = FirebaseStorage.getInstance().getReference();
        db = FirebaseFirestore.getInstance();
        uid = mAuth.getCurrentUser().getUid();

        loadUserInfo();
    }

    private void connectView(){
        toolbar = findViewById(R.id.toolbar);
        imageViewProfileImage = findViewById(R.id.image_view_profile_image);
        imageViewEditProfileImage = findViewById(R.id.image_view_edit_profile_image);
        btnSave = findViewById(R.id.btn_save);
        editTextName = findViewById(R.id.edit_text_name);
        progressBar = findViewById(R.id.progressBar);
    }

    private void addEvent(){
        imageViewEditProfileImage.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
//                startPickerImage();
                startPickerImageWithCrop();
            }
        });

        btnSave.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                doSave();
            }
        });
    }

    private void setupToolbar(){
        FirebaseUser user = FirebaseAuth.getInstance().getCurrentUser();
        toolbar.setTitle("Thiết lập tài khoản");
    }

    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
//        if(resultCode == RESULT_OK && requestCode == GALLERY_CODE){
//            image = data.getData();
//            imageViewProfileImage.setImageURI(image);
//        }

        if (requestCode == CropImage.CROP_IMAGE_ACTIVITY_REQUEST_CODE) {
            CropImage.ActivityResult result = CropImage.getActivityResult(data);
            if (resultCode == RESULT_OK) {
                image = result.getUri();
                imageViewProfileImage.setImageURI(image);
                isImageChange = true;

            } else if (resultCode == CropImage.CROP_IMAGE_ACTIVITY_RESULT_ERROR_CODE) {
                String error = result.getError().getMessage();
                Toast.makeText(SetupActivity.this, "Error: " + error, Toast.LENGTH_SHORT).show();
            }
        }
    }

    private void doSave() {
        final String name = editTextName.getText().toString();

        if (image != null && !TextUtils.isEmpty(name) && isImageChange) {
            progressBar.setVisibility(View.VISIBLE);


            final StorageReference imagePath = storageReference.child("profile_images").child(uid + ".jpg");
            final UploadTask uploadTask = imagePath.putFile(image);

            Task<Uri> urlTask = uploadTask.continueWithTask(new Continuation<UploadTask.TaskSnapshot, Task<Uri>>() {
                @Override
                public Task<Uri> then(@NonNull Task<UploadTask.TaskSnapshot> task) throws Exception {
                    if (!task.isSuccessful()) {
                        throw task.getException();
                    }

                    return imagePath.getDownloadUrl();
                }
            }).addOnCompleteListener(new OnCompleteListener<Uri>() {
                @Override
                public void onComplete(@NonNull Task<Uri> task) {
                    if (task.isSuccessful()) {
                        Uri downloadUri = task.getResult();
//                        Toast.makeText(SetupActivity.this, "Success", Toast.LENGTH_SHORT).show();
//                        Log.d("DOWNLOAD URI", downloadUri.getPath());

//                        save info profile image, name to db
                        Map<String, String> userMap = new HashMap<>();
                        userMap.put("name", name);
                        userMap.put("image", downloadUri.toString());

                        db.collection("users").document(uid).set(userMap)
                            .addOnCompleteListener(new OnCompleteListener<Void>() {
                                @Override
                                public void onComplete(@NonNull Task<Void> task) {
                                    if(task.isSuccessful()){
//                                        Toast.makeText(SetupActivity.this, "Saved", Toast.LENGTH_SHORT).show();
                                        startMainActivity();
                                    }else{
                                        String error = task.getException().getMessage();
                                        Toast.makeText(SetupActivity.this, "Failed: " + error, Toast.LENGTH_SHORT).show();
                                    }

                                    progressBar.setVisibility(View.INVISIBLE);
                                }
                            });

                    } else {
                        String error = task.getException().getMessage();
                        Toast.makeText(SetupActivity.this, "Failed: " + error, Toast.LENGTH_SHORT).show();
                        progressBar.setVisibility(View.INVISIBLE);
                    }


                }
            });
        }else{
            progressBar.setVisibility(View.VISIBLE);

            Map<String, String> userMap = new HashMap<>();
            userMap.put("name", name);


            db.collection("users").document(uid).set(userMap, SetOptions.merge()).addOnCompleteListener(new OnCompleteListener<Void>() {
                @Override
                public void onComplete(@NonNull Task<Void> task) {
                    if(task.isSuccessful()){
//                                        Toast.makeText(SetupActivity.this, "Saved", Toast.LENGTH_SHORT).show();
                        startMainActivity();
                    }else{
                        String error = task.getException().getMessage();
                        Toast.makeText(SetupActivity.this, "Failed: " + error, Toast.LENGTH_SHORT).show();
                    }

                    progressBar.setVisibility(View.INVISIBLE);
                }
            });
        }
    }

//    private  void startPickerImage(){
//        Intent galleryIntent = new Intent(Intent.ACTION_PICK);
//        galleryIntent.setType("image/*");
//        startActivityForResult(galleryIntent, GALLERY_CODE);
//    }

    private  void startPickerImageWithCrop(){
        CropImage.activity()
                .setGuidelines(CropImageView.Guidelines.ON)
                .setAspectRatio(1, 1)
                .start(SetupActivity.this);
    }

    private void startMainActivity(){
        Intent mainIntent = new Intent(SetupActivity.this, MainActivity.class);
        startActivity(mainIntent);
        finish();
    }


    private void loadUserInfo() {

        progressBar.setVisibility(View.VISIBLE);
        btnSave.setEnabled(false);

        db.collection("users").document(uid).get().addOnCompleteListener(new OnCompleteListener<DocumentSnapshot>() {
            @Override
            public void onComplete(@NonNull Task<DocumentSnapshot> task) {
                if(task.isSuccessful()){
                    if(task.getResult().exists()){
                        String name = task.getResult().getString("name");
                        String profileImage = task.getResult().getString("image");
                        image = Uri.parse(profileImage);

                        if(name != null){
                            editTextName.setText(name);
                        }

                        if(profileImage != null){
                            Glide.with(SetupActivity.this).load(profileImage).into(imageViewProfileImage);
                        }



//                        Toast.makeText(SetupActivity.this, "Loaded user's information", Toast.LENGTH_SHORT).show();
                    }else{
                        // do somethings here =))
                        String error = task.getException().getMessage();
                        Toast.makeText(SetupActivity.this, "Failed: " + error, Toast.LENGTH_SHORT).show();
                    }
                }else{
                    String error = task.getException().getMessage();
                    Toast.makeText(SetupActivity.this, "Failed: " + error, Toast.LENGTH_SHORT).show();
                }

                progressBar.setVisibility(View.INVISIBLE);
                btnSave.setEnabled(true);
            }
        });
    }
}