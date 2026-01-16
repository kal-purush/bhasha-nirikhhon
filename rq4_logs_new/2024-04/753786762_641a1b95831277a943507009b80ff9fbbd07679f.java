package com.example.fruitseason;

import android.content.Intent;
import android.os.Bundle;
import android.speech.ModelDownloadListener;
import android.util.Patterns;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import androidx.activity.EdgeToEdge;
import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.graphics.Insets;
import androidx.core.view.ViewCompat;
import androidx.core.view.WindowInsetsCompat;

import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.tasks.Task;
import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.auth.FirebaseUser;
import com.google.firebase.auth.UserProfileChangeRequest;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.ValueEventListener;

import java.util.Objects;

public class EditProfile extends AppCompatActivity {

    EditText editName, editPhone, editAddress, editCity, editProvince;
    Button cancel, save;
    String name, phone, address, city, province;
    FirebaseAuth mAuth;
    DatabaseReference reference;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_edit_profile);

        editName = findViewById(R.id.editName);
        editPhone = findViewById(R.id.editPhone);
        editAddress = findViewById(R.id.editAddress);
        editCity = findViewById(R.id.editCity);
        editProvince = findViewById(R.id.editProvince);

        cancel = findViewById(R.id.cancelEditButton);
        save = findViewById(R.id.editProfileButton);
        mAuth = FirebaseAuth.getInstance();
        reference = FirebaseDatabase.getInstance().getReference("users");

        FirebaseUser firebaseUser = mAuth.getCurrentUser();



        cancel.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(EditProfile.this, FruitsSale.class);
                startActivity(intent);
                finish();
            }
        });

        save.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                updateProfile(firebaseUser);
            }
        });

    }

    private void updateProfile(FirebaseUser firebaseUser) {
        name  = editName.getText().toString().trim();
        phone = editPhone.getText().toString().trim();
        address = editAddress.getText().toString().trim();
        city = editCity.getText().toString().trim();
        province = editProvince.getText().toString().trim();

        if (name.isEmpty() || phone.isEmpty() || address.isEmpty() || city.isEmpty()
                || province.isEmpty()){
            Toast.makeText(this, "All the fields should be filled!", Toast.LENGTH_SHORT).show();
        }
        else if(!Patterns.PHONE.matcher(phone).matches()){
            editPhone.setError("Please enter a valid phone number");
            editPhone.requestFocus();
        } else{

            Model model = new Model(name,phone,address,city,province);
            DatabaseReference reference = FirebaseDatabase.getInstance().getReference("users");
            String userId= firebaseUser.getUid();
            reference.child(userId).setValue(model).addOnCompleteListener(new OnCompleteListener<Void>() {
                @Override
                public void onComplete(@NonNull Task<Void> task) {
                    if(task.isSuccessful()){
                        UserProfileChangeRequest profileUpdates = new UserProfileChangeRequest.Builder().setDisplayName(name).build();
                        firebaseUser.updateProfile(profileUpdates);
                        Toast.makeText(EditProfile.this, "Update Successful!", Toast.LENGTH_LONG).show();
                        Intent intent = new Intent(EditProfile.this, FruitsSale.class);
                        startActivity(intent);
                    } else{
                        try{
                            throw Objects.requireNonNull(task.getException());
                        }catch(Exception e){
                            Toast.makeText(EditProfile.this, e.getMessage(), Toast.LENGTH_LONG).show();
                        }

                    }
                }
            });
        }
    }

}