package com.example.myapplication.adapters;


import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.ImageView;
import android.widget.ListView;
import android.widget.TextView;

import androidx.annotation.NonNull;

import com.example.myapplication.R;

import java.util.List;

public class BottomMenuItemsAdapter extends ArrayAdapter<Menu> {

    public BottomMenuItemsAdapter(@NonNull Context context, List<Menu> list) {
        super(context, R.layout.profile_menu_item, list);
    }

    @NonNull
    @Override
    public View getView(int position, View convertView, @NonNull ViewGroup parent) {

        Menu menu = getItem(position);

        LayoutInflater inflater = (LayoutInflater) getContext()
                .getSystemService(Context.LAYOUT_INFLATER_SERVICE);

        if (convertView == null) {
            convertView = inflater.inflate(R.layout.profile_menu_item, parent, false);
        }

        ImageView image = convertView.findViewById(R.id.icon_id);
        image.setImageResource(menu.getImg());
        TextView txt = convertView.findViewById(R.id.txt_id);
        txt.setText(menu.getTitle());

        return convertView;
    }

}