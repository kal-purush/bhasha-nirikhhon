package tr.edu.msku.steprace.activity;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.net.Uri;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.ImageView;
import android.widget.Toast;

import com.google.android.gms.tasks.OnSuccessListener;
import com.google.firebase.storage.FileDownloadTask;
import com.google.firebase.storage.FirebaseStorage;
import com.google.firebase.storage.StorageReference;
import com.google.firebase.storage.UploadTask;

import java.io.File;
import java.io.IOException;

import tr.edu.msku.steprace.R;

public class SettingsActivity extends AppCompatActivity {

    /*private FirebaseAuth mFirebaseAuth;
        private TextView email,password;*/
    //Button logout;
    Button bindir;
    FirebaseStorage storage = FirebaseStorage.getInstance();
    StorageReference reference = storage.getReference();
    ImageView viewImage;
    final File localfile =File.createTempFile("images","jpg");
    public SettingsActivity() throws IOException {

    }


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
       /* email = findViewById(R.id.email1);
        logout =findViewById(R.id.logout);*/
        /*logout.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                mFirebaseAuth.signOut();
                signOutUser();
                Log.d("dfghj","rfghjk");
            }

          *//* private void signOutUser() {
                Intent mainActivity = new Intent(SettingsActivity.this,LoginActivity.class);
                mainActivity.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_CLEAR_TASK);
                startActivity(mainActivity);

            }*//*
        });*/
       /* password =findViewById(R.id.password);
        mFirebaseAuth =FirebaseAuth.getInstance();*/
        /*        EditText name1=findViewById(R.id.name_settings);
        EditText surname1=findViewById(R.id.settings_surname);
        EditText email1 = findViewById(R.id.settings_email);
        EditText pass = findViewById(R.id.settings_password);
        email = email1.getText().toString();
        password = pass.getText().toString(); */
        setContentView(R.layout.activity_settings);
        viewImage=(ImageView) findViewById(R.id.viewImage);
        bindir =(Button) findViewById(R.id.bindir);


        bindir.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                StorageReference indir = reference.child("400340093");
                indir.getFile(localfile).addOnSuccessListener(new OnSuccessListener<FileDownloadTask.TaskSnapshot>() {
                    @Override
                    public void onSuccess(FileDownloadTask.TaskSnapshot taskSnapshot) {
                        Bitmap bitmap = BitmapFactory.decodeFile(localfile.getAbsolutePath());
                        viewImage.setImageBitmap(bitmap);



                    }
                });



            }
        });
        viewImage.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                Intent i = new Intent(Intent.ACTION_PICK);
                i.setType("image/*");
                startActivityForResult(i, 1);
            }
        });
    }

    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
        if (resultCode == RESULT_OK) {
            if (requestCode == 1) {
                Uri uri = data.getData();
                StorageReference ref = reference.child(uri.getLastPathSegment());
                ref.putFile(uri).addOnSuccessListener(new OnSuccessListener<UploadTask.TaskSnapshot>() {
                    @Override
                    public void onSuccess(UploadTask.TaskSnapshot taskSnapshot) {
                        Toast.makeText(SettingsActivity.this, "Profil Photo Updated", Toast.LENGTH_LONG).show();


                    }
                });
            }
        }
    }

}