package com.eddyfidel.marvella.character;

import android.content.Context;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import com.eddyfidel.marvella.R;
import com.eddyfidel.marvella.data.Character;
import com.squareup.picasso.Picasso;

import java.util.List;

import butterknife.BindView;
import butterknife.ButterKnife;

/**
 * Created by eddyfidel on 11/2/17.
 */

public class CharacterAdapter extends RecyclerView.Adapter<CharacterAdapter.ViewHolder> {

    private Context mContext;

    private List<Character> mCharacters;

    public CharacterAdapter(Context context, List<Character> characters) {
        mContext = context;

        setList(characters);
    }

    public void replaceData(List<Character> characters) {
        setList(characters);

        notifyDataSetChanged();
    }

    private void setList(List<Character> characters) {
        mCharacters = characters;
    }

    @Override
    public ViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        LayoutInflater inflater = LayoutInflater.from(parent.getContext());

        View rowView = inflater.inflate(R.layout.item_character, parent, false);

        ViewHolder holder = new ViewHolder(rowView);

        return holder;
    }

    @Override
    public void onBindViewHolder(ViewHolder holder, int position) {
        Character character = getItem(position);

        holder.titleTexView.setText(character.getTitle());

        Picasso.with(mContext).load(character.getThumbnail()).into(holder.thumbnailImageView);
    }

    @Override
    public int getItemCount() {
        return mCharacters.size();
    }

    public Character getItem(int position) {
        return mCharacters.get(position);
    }

    public static class ViewHolder extends RecyclerView.ViewHolder {

        @BindView(R.id.text_title) TextView titleTexView;

        @BindView(R.id.image_thumbnail) ImageView thumbnailImageView;

        public ViewHolder(View itemView) {
            super(itemView);

            ButterKnife.bind(this, itemView);
        }
    }
}