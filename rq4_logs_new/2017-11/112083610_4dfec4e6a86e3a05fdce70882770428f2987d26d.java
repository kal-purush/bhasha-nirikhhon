package com.tline.android.features.timeline.fragment.view.adapter;

import android.content.Context;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import com.tline.android.R;
import com.tline.android.features.timeline.fragment.view.holder.RecyclerViewItemHolder;
import com.twitter.sdk.android.core.models.Tweet;

import java.util.LinkedList;
import java.util.List;

public class RecyclerViewAdapter extends RecyclerView.Adapter<RecyclerViewItemHolder> {

    private List<Tweet> mList;
    private Context mContext;

    public RecyclerViewAdapter(Context context, LinkedList<Tweet> tweets) {
        this.mList = tweets;
        mContext = context;
    }

    // Return the size of your dataset (invoked by the layout manager)
    @Override
    public int getItemCount() {
        return this.mList.size();
    }


    @Override
    public RecyclerViewItemHolder onCreateViewHolder(ViewGroup viewGroup, int viewType) {

        LayoutInflater inflater = LayoutInflater.from(viewGroup.getContext());
        View view = inflater.inflate(R.layout.item_list, viewGroup, false);
        return new RecyclerViewItemHolder(mContext, view, mList);
    }

    @Override
    public void onBindViewHolder(RecyclerViewItemHolder viewHolder, int position) {

        viewHolder.bind(mList.get(position));
    }

    public void clear() {

        mList.clear();
        notifyDataSetChanged();
    }

    public void addAll(List<Tweet> list) {

        mList.addAll(list);
        notifyDataSetChanged();
    }
}