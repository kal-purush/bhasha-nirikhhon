package com.quickcanteen.quickcanteen.fragment.main;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;
import com.quickcanteen.quickcanteen.R;
import com.quickcanteen.quickcanteen.activities.canteen.CanteenActivity;
import com.quickcanteen.quickcanteen.bean.DishesBean;

import java.util.List;

public class RecommendAdapter extends RecyclerView.Adapter<RecommendAdapter.ViewHolder> implements View.OnClickListener{
    private List<DishesBean> mRecommendList;
    private Activity activity;


    static class ViewHolder extends RecyclerView.ViewHolder{
        ImageView recommendImg;
        TextView recommendName;
        TextView recommendPrice;
        private View view;

        public ViewHolder(View view) {
            super(view);
            this.view = view;
            recommendImg = (ImageView)view.findViewById(R.id.recommendImg);
            recommendName = (TextView)view.findViewById(R.id.recommendName);
            recommendPrice = (TextView)view.findViewById(R.id.recommendPrice);
        }
    }

    @Override
    public void onClick(View v) {
        //注意这里使用getTag方法获取position
        skip(mRecommendList.get((int)v.getTag()));
    }

    public RecommendAdapter(List<DishesBean> recommendList, Activity activity){
        mRecommendList = recommendList;
        this.activity = activity;
    }

    @Override
    public ViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.recommend_list,parent,false);
        ViewHolder holder = new ViewHolder(view);
        view.setOnClickListener(this);
        return holder;
    }

    @Override
    public void onBindViewHolder(ViewHolder holder, int position) {
        DishesBean recommendItem = mRecommendList.get(position);
        holder.recommendName.setText(recommendItem.getDishesName());
        holder.recommendPrice.setText("￥"+recommendItem.getPrice().toString());
        holder.recommendImg.setImageBitmap(null);//??
        holder.itemView.setTag(position);

    }

    @Override
    public int getItemCount() {
        return mRecommendList.size();
    }

    public void skip(DishesBean dishesBean){
        Intent intent = new Intent();
        Bundle bundle = new Bundle();
        bundle.putSerializable("dishesBean",dishesBean);
        intent.putExtras(bundle);
        intent.setClass(activity, CanteenActivity.class);
        startActivity(intent);
    }

    public void startActivity(Intent intent) {
        this.activity.startActivity(intent);
    }
}