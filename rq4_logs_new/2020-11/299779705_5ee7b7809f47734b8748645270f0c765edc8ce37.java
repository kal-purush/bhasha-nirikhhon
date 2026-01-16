package com.example.cmput301f20t18;

import android.os.Message;
import android.util.Log;

import androidx.annotation.NonNull;

import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.tasks.Task;
import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.firestore.CollectionReference;
import com.google.firebase.firestore.FirebaseFirestore;
import com.google.firebase.firestore.QuerySnapshot;

public class Notification {


    public static final int BORROW_REQUEST_BOOK = 1;
    public static final int CANCEL_PICKUP = 2;
    public static final int OWNER_ACCEPT_REQUEST = 3;
    public static final int OWNER_DELETE = 5;

    static final String TAG = "NOTIF_DEBUG";



    private String sourceUsername;
    private String targetUsername;
    private String bookTitle;
    private int type;
    private String message;

    // database info
    FirebaseAuth auth = FirebaseAuth.getInstance();
    FirebaseFirestore DB = FirebaseFirestore.getInstance();
    CollectionReference userRef = DB.collection("users");


    public Notification(String sourceUsername, String targetUsername, String bookTitle, int type) {
        this.sourceUsername = sourceUsername;
        this.targetUsername = targetUsername;
        this.bookTitle = bookTitle;
        this.type = type;
        this.message = "";
    }



    public void prepareMessage() {
        switch (this.type) {


            case BORROW_REQUEST_BOOK:
                this.message = String.format("%s has requested to borrow %s!", this.sourceUsername, this.bookTitle);
                break;

            case CANCEL_PICKUP:
                this.message = String.format("%s has cancelled the pickup for %s!", this.sourceUsername, this.bookTitle);
                break;


            case OWNER_ACCEPT_REQUEST:
                this.message = String.format("%s has accepted your request for %s!", this.sourceUsername, this.bookTitle);
                break;


            case OWNER_DELETE:
                this.message = String.format("%s has deleted %s from his collection, the book is now yours!", this.sourceUsername, this.bookTitle);
                break;

            default:
                Log.d(TAG, "prepareMessage: Invalid Request Type!");




        }
    }




    public void sendNotification() {

        // find the instance token of the targeted user
        userRef.whereEqualTo("username", targetUsername).get().addOnCompleteListener(new OnCompleteListener<QuerySnapshot>() {
            @Override
            public void onComplete(@NonNull Task<QuerySnapshot> task) {
                if (task.isSuccessful()) {
                    User target = task.getResult().toObjects(User.class).get(0); // username is unique and non null
                }

                else {
                    Log.d(TAG, "sendNotification - Error Finding User!");
                }
            }
        });


    }


}