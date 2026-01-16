package com.squadro.tandir;

import android.app.Activity;

import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;

import android.support.v7.app.AppCompatActivity;

import android.view.LayoutInflater;
import android.view.View;

import android.widget.Button;
import android.widget.ImageView;
import android.widget.LinearLayout;


public class AddRecipeActivity extends AppCompatActivity {

    int PICK_IMAGE_MULTIPLE = 1;

    private LayoutInflater layoutInflater;

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_add_recipe);
        layoutInflater = LayoutInflater.from(this);
        Button pickPhotos = (Button)findViewById(R.id.pickPhotos);

        pickPhotos.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent();
                intent.setType("image/*");
                intent.putExtra(Intent.EXTRA_ALLOW_MULTIPLE, true);
                intent.setAction(Intent.ACTION_GET_CONTENT);
                startActivityForResult(Intent.createChooser(intent,"Select Picture"), 1);
            }
        });




    }

    @Override
    public void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
        if(requestCode == PICK_IMAGE_MULTIPLE) {
            if(resultCode == Activity.RESULT_OK) {
                if(data.getClipData() != null) {
                    int count = data.getClipData().getItemCount(); //evaluate the count before the for loop --- otherwise, the count is evaluated every loop.
                    Uri imageUri = null;
                    for(int i = 0; i < count; i++) {
                        imageUri = data.getClipData().getItemAt(i).getUri();
                        LinearLayout linearLayout = (LinearLayout)findViewById(R.id.gallery);
                        View view = layoutInflater.inflate(R.layout.gallery_item,
                                linearLayout, false);


                        ImageView imageView = (ImageView)view.findViewById(R.id.imageView);

                        imageView.setImageURI(imageUri);


                        linearLayout.addView(view);

                    }

                }

            }
        }
    }
}
