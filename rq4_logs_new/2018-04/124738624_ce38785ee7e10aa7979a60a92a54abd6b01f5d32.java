package com.example.administrator.wuanandroid.Adapter;

import android.content.Context;
import android.support.annotation.NonNull;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import com.example.administrator.wuanandroid.Bean.SeeWeekBean.WeekNews;
import com.example.administrator.wuanandroid.R;

import java.util.ArrayList;

/**
 * 项目名：   WuanAndroid
 * 包名 ：    com.example.administrator.wuanandroid.Adapter
 * 文件名：   SeeWeekAdapter
 * 创建者:    Nixo
 * 创建时间：  2018/4/5/005-13:51
 * 描述：
 */

public class SeeWeekAdapter extends RecyclerView.Adapter<SeeWeekAdapter.ViewHolder> {

    ArrayList<WeekNews> arrayList ;
    Context context;

    public SeeWeekAdapter(Context context,ArrayList<WeekNews> arrayList) {
        this.arrayList = arrayList;
        this.context = context;
    }



    @NonNull
    @Override
    public SeeWeekAdapter.ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = null;
        if(view == null){
            view = LayoutInflater.from(context).inflate(R.layout.see_week_item,parent,false);
            return new ViewHolder(view);
        }

        return new ViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull SeeWeekAdapter.ViewHolder holder, int position) {
        WeekNews weekNews = arrayList.get(position);
        holder.title.setText(weekNews.getTitle());
        holder.neirong.setText(weekNews.getNeirong());
    }

    @Override
    public int getItemCount() {
        return arrayList.size();
    }

    public class ViewHolder extends RecyclerView.ViewHolder {

        TextView title;
        TextView neirong;

        public ViewHolder(View itemView) {
            super(itemView);
            title = itemView.findViewById(R.id.title);
            neirong = itemView.findViewById(R.id.neirong);


        }
    }
}