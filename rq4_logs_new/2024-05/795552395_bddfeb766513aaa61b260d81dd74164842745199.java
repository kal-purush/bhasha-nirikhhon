package com.hyep.movietracker.adapter;

import android.content.Context;
import android.content.res.ColorStateList;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.core.content.ContextCompat;
import androidx.core.view.ViewCompat;
import androidx.recyclerview.widget.RecyclerView;

import com.hyep.movietracker.R;
import com.hyep.movietracker.models.TagModel;

import java.util.ArrayList;

public class TagAdapter extends RecyclerView.Adapter<TagAdapter.MyViewHolder> {

    Context context;
    ArrayList<TagModel> tagModelArrayList;
    TagModel recentlyDeletedItem;
    int recentlyDeletedItemPosition;

    public TagAdapter(Context context, ArrayList<TagModel> tagModelArrayList) {
        this.context = context;
        this.tagModelArrayList = tagModelArrayList;
    }

    @NonNull

    @Override
    public TagAdapter.MyViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        LayoutInflater inflater = LayoutInflater.from(context);
        View view = inflater.inflate(R.layout.item_tag, parent, false);
        return new TagAdapter.MyViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull TagAdapter.MyViewHolder holder, int position) {

        int[] tagColors = {
                R.color.royalBlue,
                R.color.purple,
                R.color.magenta,
                R.color.aquaGreen,
                R.color.chromeYellow,
                R.color.bluePurple,
                R.color.blazeOrange,
                R.color.red,
                R.color.claret,
                R.color.smokeyGrey,
                R.color.purpleJam,
                R.color.brown,
                R.color.green,
                R.color.cobaltBlue,
                R.color.skyBlue,
        };

        holder.tvTagName.setText(tagModelArrayList.get(position).getName());

        int color = ContextCompat.getColor(holder.itemView.getContext(), tagColors[tagModelArrayList.get(position).getColor()]);
        ColorStateList colorStateList = ColorStateList.valueOf(color);
        ViewCompat.setBackgroundTintList(holder.itemView, colorStateList);
    }

    @Override
    public int getItemCount() {
        return tagModelArrayList.size();
    }

    public void deleteItem(int position) {
        recentlyDeletedItem = tagModelArrayList.get(position);
        recentlyDeletedItemPosition = position;
        tagModelArrayList.remove(position);
        notifyItemRemoved(position);
    }

    public void undoDelete() {
        tagModelArrayList.add(recentlyDeletedItemPosition, recentlyDeletedItem);
        notifyItemInserted(recentlyDeletedItemPosition);
    }

    public static class MyViewHolder extends RecyclerView.ViewHolder {

        TextView tvTagName;

        public MyViewHolder(@NonNull View itemView) {
            super(itemView);

            tvTagName = itemView.findViewById(R.id.tvTagName);
        }
    }
}