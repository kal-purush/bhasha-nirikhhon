package com.example.kabukabu;

import android.content.Context;
import android.content.Intent;
import android.database.Cursor;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.SimpleCursorAdapter;

public class CustomAdapter extends SimpleCursorAdapter implements View.OnClickListener{


    private Context mContext;
    private Context appContext;
    private int layout;
    private Cursor cr;
    private final LayoutInflater inflater;

    public interface btnListener {
        void onListBtnClick(Cursor cursor) ;
    }
    private btnListener btnListener;

    public CustomAdapter(Context context, int layout, Cursor c, String[] from, int[] to, int i, btnListener clickListener) {
        super(context, layout, c, from, to);
        this.layout = layout;
        this.mContext = context;
        this.inflater = LayoutInflater.from(context);
        this.cr = c;
        this.btnListener = clickListener ;
    }

    @Override
    public void onClick(View v) {
        if (this.btnListener != null) {
            this.btnListener.onListBtnClick((Cursor)v.getTag());
        }
    }

    @Override
    public View newView(Context context, Cursor cursor, ViewGroup parent) {
        return inflater.inflate(layout, null);

    }

    @Override
    public void bindView(View view, Context context, Cursor cursor) {
        super.bindView(view, context, cursor);
        Button btn = (Button) view.findViewById(R.id.delete_id);
        btn.setTag(cursor);
        btn.setOnClickListener(this);
    }
}