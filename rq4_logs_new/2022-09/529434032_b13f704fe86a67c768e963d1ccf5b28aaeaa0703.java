package com.example.firebasegsocapp.service;

import android.app.ProgressDialog;
import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import androidx.annotation.NonNull;
import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.tasks.OnSuccessListener;
import com.google.android.gms.tasks.Task;
import com.google.firebase.firestore.DocumentSnapshot;
import com.google.firebase.firestore.FirebaseFirestore;
import com.google.firebase.firestore.QuerySnapshot;
import com.google.firebase.storage.FirebaseStorage;
import com.google.firebase.storage.StorageReference;
import org.jetbrains.annotations.NotNull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FirebaseFilesService {

    private final int MEGABYTE = 1024*1024;
    private static String[] acceptedMimeTypes;
    private static HashMap<String, String> fileExtensionsMimes = new HashMap<>();
    private static HashMap<String, Bitmap> fileExtensionsImages = new HashMap<>();
    private static int fileTypesToLoad = 0;
    private static int loadedFileTypes = 0;

    private final FirebaseFirestore FIREBASE_FIRESTORE = FirebaseFirestore.getInstance();
    private Context context;

    public FirebaseFilesService() {
    }

    public FirebaseFilesService(Context context) {
        this.context = context;
    }

    public HashMap<String,String> getFileExtensionsMimes(){
        return fileExtensionsMimes;
    }

    public HashMap<String,Bitmap> getFileExtensionsImages(){
        return fileExtensionsImages;
    }

    public String[] loadAcceptedMimeTypesAndResources(){
        ProgressDialog progressDialog = new ProgressDialog(context);
        progressDialog.setMessage("Starting app.\nPlease wait ...");
        progressDialog.setCancelable(false);
        progressDialog.show();

        FIREBASE_FIRESTORE.collection("accepted_file_types").get().addOnCompleteListener(new OnCompleteListener<QuerySnapshot>() {
            @Override
            public void onComplete(@NonNull @NotNull Task<QuerySnapshot> task) {
                if (task.isSuccessful()) {
                    List<String> mimeTypes = new ArrayList<>();
                    fileTypesToLoad = task.getResult().size();
                    for(DocumentSnapshot ds: task.getResult()){
                        String fileMimeType = ds.getString("mime_type");
                        String fileImageResource = ds.getString("image_resource");
                        String fileExtension = ds.getString("file_extension");

                        mimeTypes.add(fileMimeType);
                        fileExtensionsMimes.put(fileExtension, fileMimeType);

                        StorageReference storageReference = FirebaseStorage.getInstance().getReference().child("image-resources");
                        StorageReference fileReference = storageReference.child(fileImageResource+".png");

                        fileReference.getBytes(MEGABYTE).addOnSuccessListener(new OnSuccessListener<byte[]>() {
                            @Override
                            public void onSuccess(byte[] bytes) {
                                loadedFileTypes++;
                                Bitmap bmp=BitmapFactory.decodeByteArray(bytes,0,bytes.length);
                                fileExtensionsImages.put(fileExtension, bmp);
                                if(loadedFileTypes==fileTypesToLoad){
                                    loadedFileTypes=0;
                                    fileTypesToLoad=0;
                                    progressDialog.dismiss();
                                }
                            }
                        });
                    }
                    acceptedMimeTypes = new String[mimeTypes.size()];
                    mimeTypes.toArray(acceptedMimeTypes);
                }
            }
        });
        return acceptedMimeTypes;
    }

}