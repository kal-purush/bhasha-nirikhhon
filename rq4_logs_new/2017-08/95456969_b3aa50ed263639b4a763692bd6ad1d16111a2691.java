package com.example.edelsteindo.androidproject.Model;

import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.ValueEventListener;

/**
 * Created by edelsteindo on 07/08/2017.
 */

public class FirebaseModel {

    public void addPost(Post post){
        FirebaseDatabase database = FirebaseDatabase.getInstance();
        DatabaseReference myRef = database.getReference("posts");
        myRef.child(post.getId()).setValue(post);
    }

    public void removePost(String id){
        FirebaseDatabase database = FirebaseDatabase.getInstance();
        DatabaseReference myRef = database.getReference("posts");
        myRef.child(id).removeValue();
    }
}