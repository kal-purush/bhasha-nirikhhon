package com.example.enigmator.controller;

import android.content.Context;
import android.support.annotation.NonNull;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import com.example.enigmator.R;
import com.example.enigmator.entity.Enigma;
import com.example.enigmator.fragment.EnigmaFragment;

import java.util.List;

public class EnigmaRecyclerViewAdapter extends RecyclerView.Adapter<EnigmaRecyclerViewAdapter.ViewHolder> {
    private List<Enigma> mValues;
    private final EnigmaFragment.OnListFragmentInteractionListener mListener;
    private final Context context;

    public EnigmaRecyclerViewAdapter(Context context, List<Enigma> enigmas, EnigmaFragment.OnListFragmentInteractionListener listener) {
        super();
        this.context = context;
        mValues = enigmas;
        mListener = listener;
    }

    @NonNull
    @Override
    public ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext())
                .inflate(R.layout.enigma_item, parent, false);
        return new ViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull final ViewHolder holder, int position) {
        Enigma enigma = mValues.get(position);

        holder.mItem = enigma;
        holder.mTitle.setText(enigma.getName());
        holder.mValue.setText(context.getString(R.string.enigma_score, enigma.getScoreReward()));

        holder.mView.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                if (null != mListener) {
                    // Notify the active callbacks interface (the activity, if the
                    // fragment is attached to one) that an item has been selected.
                    mListener.onListFragmentInteraction(v, holder.mItem);
                }
            }
        });
    }

    @Override
    public int getItemCount() {
        return mValues.size();
    }

    public void setValues(List<Enigma> values) {
        this.mValues = values;
    }

    class ViewHolder extends RecyclerView.ViewHolder {
        private final TextView mTitle;
        private final TextView mValue;
        private final View mView;
        private Enigma mItem;

        private ViewHolder(View view) {
            super(view);
            mView = view;
            mTitle = view.findViewById(R.id.text_enigma_title);
            mValue = view.findViewById(R.id.text_enigma_value);
        }
    }
}