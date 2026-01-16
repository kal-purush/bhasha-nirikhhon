package com.mum6ojumbo.locateme.services;

import android.app.Application;
import android.app.Service;
import android.arch.lifecycle.LiveData;
import android.content.Intent;
import android.os.IBinder;
import android.support.annotation.Nullable;
import android.util.Log;
import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.database.ChildEventListener;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.Query;
import com.google.firebase.database.ValueEventListener;
import com.mum6ojumbo.locateme.model.LocationSharedByUsersEntity;
import com.mum6ojumbo.locateme.room.repositories.FirebaseDatabaseRecRepository;
import java.util.List;


public class LocationReceiverService extends Service {
    DatabaseReference mDatabase;
    FirebaseDatabaseRecRepository mFirebaseDatabaseRecRepo;
    private final String TAG="LocationReceiverService";
    @Nullable
    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }

    public LocationReceiverService(){
        super();

    }

    public void setupFirebaseNodeListener(){
        Log.i(TAG,FirebaseAuth.getInstance().getUid()+"setupFIREBASELISTENER");
        Query q=mDatabase.child("Sharing");
        q.addValueEventListener(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                Log.i(TAG,"getting Data");
                for(DataSnapshot singleSnapshot:dataSnapshot.getChildren()){
                    LocationSharedByUsersEntity sharedByUsersEntity =
                            singleSnapshot.getValue(LocationSharedByUsersEntity.class);
                    mFirebaseDatabaseRecRepo.insertRecord(sharedByUsersEntity);
                    //listLocationHistory();
                }
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });


    }



    @Override
    public int onStartCommand(Intent intent,int flags,int startId){
        Log.i(TAG,"onStartCommand");
        mFirebaseDatabaseRecRepo= new FirebaseDatabaseRecRepository( getApplication());
        mDatabase=FirebaseDatabase.getInstance().getReference("LocationsHistory").child(FirebaseAuth.getInstance().getUid());
        setupFirebaseNodeListener();
        return START_STICKY;
    }
    @Override
    public void onDestroy(){
        super.onDestroy();
        Log.i(TAG,"onDestory");
        //locationRepository.stopLocationUpdates();
    }
}