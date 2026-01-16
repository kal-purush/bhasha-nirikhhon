package io.juismi;

import android.app.Activity;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;

import com.google.firebase.database.ChildEventListener;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.Query;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by JARVIS on 23/02/2018.
 */

public abstract class FirebaseAdapter<T> extends BaseAdapter {

    private Query mRef;
    private int layoutID;
    private LayoutInflater inflater;
    private ChildEventListener listener;
    private Class<T> model;
    private List<T> models;
    private List<String> keys;

    public FirebaseAdapter(Query mRef, Class<T> mModelClass, int mLayout, Activity activity){
        this.mRef = mRef;
        this.model = mModelClass;
        this.layoutID = mLayout;
        this.inflater = activity.getLayoutInflater();
        this.models = new ArrayList<T>();
        this.keys = new ArrayList<String>();
        listener = this.mRef.addChildEventListener(new ChildEventListener() {
            @Override
            public void onChildAdded(DataSnapshot dataSnapshot, String s) {
                Log.wtf("hola", "Entered added");
                T model = dataSnapshot.getValue(FirebaseAdapter.this.model);
                String key = dataSnapshot.getKey();

                if(s == null){
                    models.add(0, model);
                    keys.add(0, key);
                }
                else{
                    int previousIndex = keys.indexOf(s);
                    int nextIndex = previousIndex + 1;
                    if (nextIndex == models.size()) {
                        models.add(model);
                        keys.add(key);
                    }
                    else{
                        models.add(nextIndex, model);
                        keys.add(nextIndex, key);
                    }
                }

                notifyDataSetChanged();
            }

            @Override
            public void onChildChanged(DataSnapshot dataSnapshot, String s) {
                Log.wtf("hola", "Entered changed");
                String key = dataSnapshot.getKey();
                T newModel = dataSnapshot.getValue(FirebaseAdapter.this.model);
                int index = keys.indexOf(key);
                models.set(index, newModel);

                notifyDataSetChanged();
            }

            @Override
            public void onChildRemoved(DataSnapshot dataSnapshot) {
                Log.wtf("hola", "Entered removed");
                String key = dataSnapshot.getKey();
                int index = keys.indexOf(key);

                keys.remove(index);
                models.remove(index);

                notifyDataSetChanged();
            }

            @Override
            public void onChildMoved(DataSnapshot dataSnapshot, String s) {
                Log.wtf("hola", "Entered moved");
                String key = dataSnapshot.getKey();
                T newModel = dataSnapshot.getValue(FirebaseAdapter.this.model);
                int index = keys.indexOf(key);
                models.remove(index);
                keys.remove(index);
                if (s == null) {
                    models.add(0, newModel);
                    keys.add(0, key);
                } else {
                    int previousIndex = keys.indexOf(s);
                    int nextIndex = previousIndex + 1;
                    if (nextIndex == models.size()) {
                        models.add(newModel);
                        keys.add(key);
                    } else {
                        models.add(nextIndex, newModel);
                        keys.add(nextIndex, key);
                    }
                }
                notifyDataSetChanged();
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {
                Log.e("FirebaseListAdapter", "Listen was cancelled, no more updates will occur");
            }
        });
    }

    public void cleanup() {
        // We're being destroyed, let go of our mListener and forget about all of the mModels
        mRef.removeEventListener(listener);
        models.clear();
        keys.clear();
    }

    @Override
    public int getCount() {
        return this.models.size();
    }

    @Override
    public Object getItem(int i) {
        return this.models.get(i);
    }

    @Override
    public long getItemId(int i) {
        return i;
    }

    @Override
    public View getView(int i, View view, ViewGroup viewGroup) {
        if (view == null){
            view = inflater.inflate(layoutID, viewGroup, false);

        }

        T model = models.get(i);
        populateView(view, model);
        return view;
    }

    protected abstract void populateView(View v, T model);
}