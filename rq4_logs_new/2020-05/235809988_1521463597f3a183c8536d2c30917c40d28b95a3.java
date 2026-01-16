package com.group4sweng.scranplan.Social.Messenger;

import androidx.appcompat.app.AppCompatActivity;

import com.google.firebase.firestore.CollectionReference;
import com.google.firebase.firestore.FirebaseFirestore;
import com.google.firebase.storage.FirebaseStorage;
import com.google.firebase.storage.StorageReference;
import com.group4sweng.scranplan.UserInfo.UserInfoPrivate;

/***
 * Class to View active messages between people
 */
public class MessengerMenu extends AppCompatActivity {

    UserInfoPrivate mUser;

    // Database objects for accessing recipes
    private FirebaseFirestore mDatabase = FirebaseFirestore.getInstance();
    private CollectionReference mColRef = mDatabase.collection("followers");

    // Firebase user collection and storage references.
    CollectionReference mRef = mDatabase.collection("posts");
    FirebaseStorage mStorage = FirebaseStorage.getInstance();
    StorageReference mStorageReference = mStorage.getReference();



}