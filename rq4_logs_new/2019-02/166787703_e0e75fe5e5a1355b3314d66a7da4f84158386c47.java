package com.example.mobile_client;

import android.widget.ArrayAdapter;
import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.ImageView;
import android.widget.TextView;
import java.util.List;

public class FileAdapter extends ArrayAdapter <File_icon> {
    private int resourceId;
    public FileAdapter(Context context, int textViewResourceId,List<File_icon> objects) {
        super(context, textViewResourceId, objects);
        resourceId = textViewResourceId;
    }




    @Override
    public View getView(int position, View convertView, ViewGroup parent) {
        File_icon file = getItem(position); // 获取当前项的Fruit实例
        View view;
        ViewHolder viewHolder;
        if (convertView == null) {
            view = LayoutInflater.from(getContext()).inflate(resourceId, parent, false);
            viewHolder = new ViewHolder();
            viewHolder.iconImage = (ImageView) view.findViewById (R.id.File_icon);
            viewHolder.fileName =(TextView) view.findViewById(R.id.file_list);
            view.setTag(viewHolder); // 将ViewHolder存储在View中
        } else {
            view = convertView;
            viewHolder = (ViewHolder) view.getTag(); // 重新获取ViewHolder
        }
        viewHolder.iconImage.setImageResource(file.getIconId());
        viewHolder.fileName.setText(file.getName());
        return view;
    }

    class ViewHolder {

        ImageView iconImage;

        TextView fileName;

    }
}