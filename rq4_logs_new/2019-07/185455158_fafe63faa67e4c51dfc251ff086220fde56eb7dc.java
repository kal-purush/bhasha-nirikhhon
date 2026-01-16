package com.hotsauce.meem;

import android.content.Context;
import android.content.Intent;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.os.Environment;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;

import androidx.recyclerview.widget.RecyclerView;

import com.hotsauce.meem.db.Meme;

import java.io.File;
import java.util.Collections;
import java.util.List;

public class MemeViewAdapter extends RecyclerView.Adapter<MemeViewAdapter.MemeViewHolder> {

    class MemeViewHolder extends RecyclerView.ViewHolder {
        private final ImageView memeItemView;

        private MemeViewHolder(View itemView) {
            super(itemView);
            memeItemView = itemView.findViewById(R.id.GalleryCell);
        }
    }

    private final LayoutInflater inflater;
    private List<Meme> memes = Collections.emptyList();
    private Context context;

    MemeViewAdapter(Context context) {
        this.inflater = LayoutInflater.from(context);
        this.context = context;
    }

    @Override
    public MemeViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        View view = inflater.inflate(R.layout.gallery_cell, parent, false);
        return new MemeViewHolder(view);
    }

    @Override
    public void onBindViewHolder(MemeViewHolder holder, int position) {
        holder.memeItemView.setScaleType(ImageView.ScaleType.CENTER_CROP);
        Bitmap memeBitmap = BitmapFactory.decodeFile(getFilepath(this.memes.get(position)));
        holder.memeItemView.setImageBitmap(memeBitmap);

        // create touch event
        final String current_image_path = getFilepath(this.memes.get(position));
        holder.memeItemView.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Intent intent = new Intent(context, GalleryActionActivity.class);
                intent.putExtra("image_path", current_image_path);
                Log.d("memePath", current_image_path);
                context.startActivity(intent);
            }
        });
    }

    void setMemes(List<Meme> memes) {
        this.memes = memes;
        notifyDataSetChanged();
    }

    //
    @Override
    public int getItemCount() {
        return this.memes.size();
    }

    public static String getFilepath(Meme meme) {
        return Environment.getExternalStorageDirectory().getAbsolutePath() + File.separator + meme.getId() + ".png";
    }

}