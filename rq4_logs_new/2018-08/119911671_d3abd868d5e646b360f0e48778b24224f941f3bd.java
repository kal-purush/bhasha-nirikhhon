package com.example.android.popularmovies;

import android.content.Context;
import android.support.annotation.NonNull;
import android.support.v7.widget.RecyclerView;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import java.util.ArrayList;
import java.util.HashMap;

public class ReviewAdapter extends RecyclerView.Adapter<ReviewAdapter.ReviewViewHolder> {
    private static String TAG = ReviewAdapter.class.getSimpleName();

    private ArrayList<HashMap<String, String>> mReviewData;


    public ReviewAdapter(@NonNull Context context) {

    }

    @Override
    public ReviewViewHolder onCreateViewHolder(ViewGroup viewGroup, int viewType) {
        Context context = viewGroup.getContext();
        int layoutIdForItem = R.layout.review_view_item;
        LayoutInflater inflater = LayoutInflater.from(context);
        boolean shouldAttachToParentImmediately = false;

        View view = inflater.inflate(layoutIdForItem, viewGroup, shouldAttachToParentImmediately);
        ReviewViewHolder viewHolder = new ReviewViewHolder(view);

        return viewHolder;
    }

    @Override
    public void onBindViewHolder(ReviewViewHolder viewHolder, int position) {
        HashMap<String, String> reviewDataMap = mReviewData.get(position);
        String author = reviewDataMap.get("AUTHOR");
        String content = reviewDataMap.get("CONTENT");
        Log.d(TAG, "Review bind view holder got author and content " + author + content);
        viewHolder.reviewContentTextView.setText("Author: " + author);
        viewHolder.reviewAuthorTextView.setText(content);
    }

    @Override
    public int getItemCount() {
        if (mReviewData == null) return 0;
        return mReviewData.size(); }

    class ReviewViewHolder extends RecyclerView.ViewHolder {
        TextView reviewAuthorTextView;
        TextView reviewContentTextView;

        public ReviewViewHolder(View itemView) {
            super(itemView);

            reviewAuthorTextView = (TextView) itemView.findViewById(R.id.review_author);
            reviewContentTextView = (TextView) itemView.findViewById(R.id.review_content);
        }
    }
    public void setReviewData(ArrayList<HashMap<String, String>> reviewData) {
        mReviewData = reviewData;
        notifyDataSetChanged();
    }
}