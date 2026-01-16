package com.example.oguz.whatsaround;

import android.content.Context;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.text.Layout;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.TextView;

import java.util.List;

/**
 * Created by oguz on 24.05.2017.
 */

public class CompanyListViewAdapter extends ArrayAdapter<Company> {

    private View.OnClickListener onClickListener;

    public CompanyListViewAdapter(Context context, List<Company> companies) {
        super(context, R.layout.company_list_item, companies);
        //this.onClickListener = onClickListener;
    }

    @NonNull
    @Override
    public View getView(int position, @Nullable View convertView, @NonNull ViewGroup parent) {
        View view = convertView;

        TextView tvName, tvPhone, tvEmail;

        if (view == null) {
            LayoutInflater layoutInflater = LayoutInflater.from(getContext());
            view = layoutInflater.inflate(R.layout.company_list_item, parent, false);

            tvName = (TextView) view.findViewById(R.id.tvCompName);
            tvPhone = (TextView) view.findViewById(R.id.tvPhone);
            tvEmail = (TextView) view.findViewById(R.id.tvEmail);

            Company comp = getItem(position);
            tvName.setText(comp.getFormattedName());
            tvEmail.setText(comp.getEmail());
            tvPhone.setText(comp.getPhone());

        }
        return view;
    }
}