package com.delose.onlineshopph;

import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.app.Fragment;
import android.util.Patterns;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;

import com.facebook.drawee.view.SimpleDraweeView;
import com.google.android.gms.tasks.OnSuccessListener;
import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.ValueEventListener;
import com.google.firebase.storage.FirebaseStorage;
import com.google.firebase.storage.StorageReference;

public class AccountFragment extends Fragment {

    Long count;

    private Button btnLogout,btnSubscribe;
    private SimpleDraweeView imgCover,imgUser;
    private EditText txtEmailSubscribe;
    private TextView txtDisplayEmail;

    private FirebaseAuth firebaseAuth;
    private FirebaseStorage firebaseStorage;
    private DatabaseReference databaseReference;
    private StorageReference storageReference;


    public AccountFragment() {
        // Required empty public constructor
    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        // Inflate the layout for this fragment
        return inflater.inflate(R.layout.fragment_account, container, false);
    }

    @Override
    public void onActivityCreated(Bundle savedInstance)
    {
        super.onActivityCreated(savedInstance);

        firebaseAuth = FirebaseAuth.getInstance();

        btnLogout = getView().findViewById(R.id.button_logout);
        btnSubscribe = getView().findViewById(R.id.button_subscribe);
        imgCover = getView().findViewById(R.id.image_cover);
        imgUser = getView().findViewById(R.id.image_user);
        txtEmailSubscribe = getView().findViewById(R.id.text_subscribe);
        txtDisplayEmail = getView().findViewById(R.id.text_email);

        firebaseStorage = FirebaseStorage.getInstance();
        storageReference = firebaseStorage.getReference();
        databaseReference = FirebaseDatabase.getInstance().getReference();

        btnLogout.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                firebaseAuth.signOut();
                Intent intentLoginActivity = new Intent(getActivity(),LoginActivity.class);
                startActivity(intentLoginActivity);
            }
        });

        btnSubscribe.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                subscribe();
            }
        });

        storageReference.child("default_cover.jpg").getDownloadUrl().addOnSuccessListener(new OnSuccessListener<Uri>() {
            @Override
            public void onSuccess(Uri uri)
            {
                imgCover.setImageURI(uri);
            }
        });

        storageReference.child("default_user.png").getDownloadUrl().addOnSuccessListener(new OnSuccessListener<Uri>() {
            @Override
            public void onSuccess(Uri uri)
            {
                imgUser.setImageURI(uri);
            }
        });

        txtDisplayEmail.setText(firebaseAuth.getCurrentUser().getEmail().toString());

    }

    private void subscribe()
    {
        final String strEmail = txtEmailSubscribe.getText().toString().trim();

        if(strEmail.isEmpty())
        {
            Toast.makeText(getActivity(), "CANNOT SUBSCRIBE WITH NO EMAIL", Toast.LENGTH_SHORT).show();
            return;
        }

        if(!Patterns.EMAIL_ADDRESS.matcher(strEmail).matches())
        {
            Toast.makeText(getActivity(),"CANNOT SUBSCRIBE WITH INVALID EMAIL",Toast.LENGTH_SHORT).show();
            return;
        }


        databaseReference.child("mailing_list").addValueEventListener(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                 count=dataSnapshot.getChildrenCount();
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {
                
            }

        });

        if(count==null)
        {
            count=1L;
        }

        else
        {
            count++;
        }
        databaseReference.child("mailing_list").child(count.toString()).setValue(strEmail).addOnSuccessListener(new OnSuccessListener<Void>() {
            @Override
            public void onSuccess(Void aVoid) {
                Toast.makeText(getActivity(), "SUBSCRIBED!", Toast.LENGTH_SHORT).show();
            }
        });
    }
}