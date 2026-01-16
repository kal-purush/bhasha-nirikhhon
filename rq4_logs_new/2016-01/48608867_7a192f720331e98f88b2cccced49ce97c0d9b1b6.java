package com.saint.aoyangbuulid.mine.adapter;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.TextView;

import com.saint.aoyangbuulid.R;

import java.util.List;
import java.util.Map;

/**
 * Created by zzh on 15-12-31.
 */
public class PostedAdapter extends BaseAdapter {
    private Context context;
    private List<Map<String , Object>> list;
    public PostedAdapter(Context context,List list){
        this.context=context;
        this.list=list;

    }
    @Override
    public int getCount() {
        return list.size();
    }

    @Override
    public Object getItem(int position) {
        return list.get(position);
    }

    @Override
    public long getItemId(int position) {
        return position;
    }

    @Override
    public View getView(int position, View convertView, ViewGroup parent) {
        Holder holder=null;
        if (convertView==null){
            convertView= LayoutInflater.from(context).inflate(R.layout.post_main,null,true);
            holder=new Holder();
            holder.text_content= (TextView)convertView.findViewById(R.id.text_content);
            holder.text_date= (TextView) convertView.findViewById(R.id.text_time);
            holder.text_title= (TextView) convertView.findViewById(R.id.text_title);
            convertView.setTag(holder);
        }else {
            holder= (Holder) convertView.getTag();
        }


        return convertView;
    }
    private class Holder{
        TextView text_title,text_date,text_content;

    }
}