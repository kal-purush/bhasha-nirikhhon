package com.ufo.ufomobile.reportesmoviles;

import android.content.Context;
import android.content.Intent;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.net.Uri;
import android.os.Bundle;
import android.provider.MediaStore;
import android.support.design.widget.FloatingActionButton;
import android.support.design.widget.Snackbar;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.support.v7.widget.Toolbar;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.MotionEvent;
import android.view.View;
import android.view.ViewGroup;
import android.widget.EditText;
import android.widget.ImageView;
import android.widget.ScrollView;
import android.widget.TextView;
import android.widget.Toast;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import utilities.Report;

public class AddNewReportActivity extends AppCompatActivity {

    private int categoryResource;

    private ImageView categoryImg;
    private EditText title,description,address,referencePoint;
    private ScrollView scrollView;
    Recycler_View_Adapter adapter;
    List<Bitmap> data;

    int total_imgs;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_add_new_report);
        Toolbar toolbar = (Toolbar) findViewById(R.id.toolbar);
        setSupportActionBar(toolbar);
        getSupportActionBar().setDisplayHomeAsUpEnabled(true);

        scrollView=(ScrollView)findViewById(R.id.scrollView);

        categoryResource = getIntent().getIntExtra("category_icon",0);
        categoryImg=(ImageView)findViewById(R.id.category__img);
        categoryImg.setImageResource(categoryResource);

        description=(EditText)findViewById(R.id.description);
        description.setOnTouchListener(new View.OnTouchListener() {
            @Override
            public boolean onTouch(View view, MotionEvent motionEvent) {
                scrollView.requestDisallowInterceptTouchEvent(true);
                return false;
            }
        });


        //Horizontal list -------------------------------------------------------------------------
        //----------------------------------------------------------------------------------------
        data = new ArrayList<>();
        data.add(null);
        //----------------------------------------------------------------------------------------
        LinearLayoutManager layoutManager = new LinearLayoutManager(this, LinearLayoutManager.HORIZONTAL, false);
        RecyclerView myList = (RecyclerView) findViewById(R.id.img_recycler_view);
        adapter = new Recycler_View_Adapter(data, getApplication());
        myList.setAdapter(adapter);
        myList.setLayoutManager(layoutManager);
    }

    protected void onActivityResult(int requestCode, int resultCode, Intent imageReturnedIntent) {
        super.onActivityResult(requestCode, resultCode, imageReturnedIntent);
        switch(requestCode) {
            case 0:
                if(resultCode == RESULT_OK){
                    Uri selectedImage = imageReturnedIntent.getData();
                    try {
                        Bitmap bp = decodeUri(getApplicationContext(), selectedImage);
                        int w=256;
                        int h=256;
                        adapter.insert(0, Bitmap.createScaledBitmap(bp, w, h, false));
                        total_imgs++;
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }

                break;
            case 1:
                if(resultCode == RESULT_OK){
                    Uri selectedImage = imageReturnedIntent.getData();
                    try {
                        Bitmap bp = decodeUri(getApplicationContext(), selectedImage);
                        //adapter.insert(0, Bitmap.createScaledBitmap(bp, w, h, false));
                        //adapter.insert(0,null);
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
                break;
        }
    }

    public static Bitmap decodeUri(Context c, Uri uri)
            throws FileNotFoundException {
        BitmapFactory.Options o = new BitmapFactory.Options();
        return BitmapFactory.decodeStream(c.getContentResolver().openInputStream(uri), null, o);
    }


    //--------------------------------------------------------------------------------------------------------------
    //RECYCLER VIEW ADAPTER FOR HORIZONTAL SCROLL
    //--------------------------------------------------------------------------------------------------------------
    public class Recycler_View_Adapter extends RecyclerView.Adapter<View_Holder> {

        List<Bitmap> list = Collections.emptyList();
        Context context;

        public Recycler_View_Adapter(List<Bitmap> list, Context context) {
            this.list = list;
            this.context = context;
        }

        @Override
        public View_Holder onCreateViewHolder(ViewGroup parent, int viewType) {
            //Inflate the layout, initialize the View Holder
            View v = LayoutInflater.from(parent.getContext()).inflate(R.layout.image_row_layout, parent, false);
            View_Holder holder = new View_Holder(v);

            return holder;

        }

        @Override
        public void onBindViewHolder(View_Holder holder, final int position) {

             //Use the provided View Holder on the onCreateViewHolder method to populate the current row on the RecyclerView
            if(list.get(position)!=null){
                holder.image.setImageBitmap(list.get(position));
            }
            else{
                holder.image.setBackgroundResource(R.drawable.add_image_background);
                holder.image.setOnClickListener(new View.OnClickListener() {
                    @Override
                    public void onClick(View v) {
                        //data.add(0,null);
                        //adapter.insert(0,null);
                        if(total_imgs<4) {
                            Intent takePicture = new Intent(MediaStore.ACTION_IMAGE_CAPTURE);
                            startActivityForResult(takePicture, 0);
                        }else{
                            Toast.makeText(getApplicationContext(),
                                    "Númer máximo de imagenes alcanzado",Toast.LENGTH_SHORT).show();
                        }
                    }
                });
            }
            //holder.image.setText(list.get(position).getTitle());

            //animate(holder);

        }

        @Override
        public int getItemCount() {
            //returns the number of elements the RecyclerView will display
            return list.size();
        }

        @Override
        public void onAttachedToRecyclerView(RecyclerView recyclerView) {
            super.onAttachedToRecyclerView(recyclerView);
        }

        // Insert a new item to the RecyclerView on a predefined position
        public void insert(int position, Bitmap data) {
            list.add(position, data);
            notifyItemInserted(position);
        }

        // Remove a RecyclerView item containing a specified Data object
        public void remove(String data) {
            int position = list.indexOf(data);
            list.remove(position);
            notifyItemRemoved(position);
        }

    }

    //Auxiliar class holder ------------------------------------------------------------
    public class View_Holder extends RecyclerView.ViewHolder {

        ImageView image;

        View_Holder(View itemView) {
            super(itemView);
            image = (ImageView) itemView.findViewById(R.id.image);
        }
    }

}