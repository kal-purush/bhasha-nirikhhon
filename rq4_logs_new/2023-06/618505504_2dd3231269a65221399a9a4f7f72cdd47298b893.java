package com.example.mobileappprogrammingproject;

import android.content.Context;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.CheckBox;
import android.widget.TextView;

import com.example.mobileappprogrammingproject.ObjectJSON.TaiKhoanNganHangJSONObject;

import java.util.ArrayList;

public class NganhangAdapter extends BaseAdapter{
    private Context context;
    private int layout;
    ArrayList<TaiKhoanNganHangJSONObject> listNH;
    int selectedPosition = -1;
    String selectedStrings = "";
    String selectedBankName;
    public NganhangAdapter(Context context, int layout, ArrayList<TaiKhoanNganHangJSONObject> listNH) {
        this.context = context;
        this.layout = layout;
        this.listNH = listNH;
    }

    @Override
    public int getCount() {
        return listNH.size();
    }

    @Override
    public Object getItem(int i) {
        return null;
    }

    @Override
    public long getItemId(int i) {
        return 0;
    }


    private class ViewHolder
    {
        TextView txtTenNganHang;
        CheckBox cbNganHang;
        public ViewHolder(View v) {
            txtTenNganHang = (TextView) v.findViewById(R.id.txtTenNganHang);
            cbNganHang = (CheckBox) v.findViewById(R.id.cbNganHang);
        }
    }

    @Override
    public View getView(int i, View view, ViewGroup viewGroup) {
        ViewHolder viewHolder;
        LayoutInflater inflater = (LayoutInflater) context.getSystemService(Context.LAYOUT_INFLATER_SERVICE);

        if (view==null)
        {
            view = inflater.inflate(R.layout.nganhang_item, viewGroup, false);
            viewHolder = new ViewHolder(view);
            view.setTag(viewHolder);
        }
        else
        {
            viewHolder = (ViewHolder) view.getTag();
        }
        TaiKhoanNganHangJSONObject nh = listNH.get(i);
        viewHolder.txtTenNganHang.setText(nh.getTennh());
        viewHolder.cbNganHang.setTag(selectedPosition);

        if (i == selectedPosition) {
            viewHolder.cbNganHang.setChecked(true);
        } else {
            viewHolder.cbNganHang.setChecked(false);
        }
        viewHolder.cbNganHang.setOnClickListener(onStateChangedListener(viewHolder.cbNganHang, i));

        return view;
    }
    private View.OnClickListener onStateChangedListener(final CheckBox checkBox, final int position) {
        return new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                if (checkBox.isChecked()) {
                    selectedPosition = position;
                    selectedStrings = listNH.get(position).getMatk();
                    selectedBankName = listNH.get(position).getTennh();
                    Log.e("TenNh", selectedStrings);
                } else {
                    selectedPosition = -1;
                }
                notifyDataSetChanged();
                GECL.print("sfldskjfls");
            }
        };
    }
    public String[] getSelectedInfo(){
//        return selectedStrings;
        return new String[]{selectedStrings, selectedBankName, String.valueOf(selectedPosition)};
    }
}