package com.example.its_a_feature_not_a_bug;

import android.app.AlertDialog;
import android.app.Dialog;
import android.content.Context;
import android.content.DialogInterface;
import android.os.Bundle;
import android.provider.Settings;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.fragment.app.DialogFragment;

import com.google.android.gms.tasks.OnFailureListener;
import com.google.android.gms.tasks.OnSuccessListener;
import com.google.firebase.firestore.CollectionReference;
import com.google.firebase.firestore.FirebaseFirestore;

import java.util.HashMap;
import java.util.Map;

public class NewUserFragment extends DialogFragment {
    private AddEventDialogueListener listener;
    private FirebaseFirestore db;
    private CollectionReference usersRef;

    public NewUserFragment(){}

    @NonNull
    @Override
    public Dialog onCreateDialog(@Nullable Bundle savedInstanceState) {
        View view = LayoutInflater.from(getContext()).inflate(R.layout.fragment_new_user, null);

        EditText editName = view.findViewById(R.id.edit_text_new_user_name);

        AlertDialog.Builder builder = new AlertDialog.Builder(getContext());
        AlertDialog alertDialog = builder
                .setView(view)
                .setTitle("New User")
                .setPositiveButton("ok", null) // Delaying positive button action to handle Switch change
                .create();

        alertDialog.setOnShowListener(dialog -> {
            Button positiveButton = alertDialog.getButton(DialogInterface.BUTTON_POSITIVE);
            positiveButton.setOnClickListener(v -> {
                String name = editName.getText().toString();
                addUsertoDatabase(name);

                alertDialog.dismiss();
            });
        });
        return alertDialog;
    }

    public void addUsertoDatabase(String name) {
        // get android id
        Context context = getActivity();
        String androidId = Settings.Secure.getString(context.getContentResolver(), Settings.Secure.ANDROID_ID);

        // connect to database
        db = FirebaseFirestore.getInstance();
        usersRef = db.collection("profiles");

        // put data in hash map
        Map<String, Object> data = new HashMap<>();
        data.put("fullName", name);

        usersRef.document(androidId)
                .set(data)
                .addOnSuccessListener(new OnSuccessListener<Void>() {
                    @Override
                    public void onSuccess(Void unused) {
                        Log.d("Firestore", "DocumentSnapshot successfully written");
                    }
                }).addOnFailureListener(new OnFailureListener() {
                    @Override
                    public void onFailure(@NonNull Exception e) {
                        Log.e("Firestore", "Failed to upload event", e);
                    }
                });
    }
}