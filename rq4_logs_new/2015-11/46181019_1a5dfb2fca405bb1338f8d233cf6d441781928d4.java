package com.smapley.powerwork.adapter;

import android.content.Context;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import com.smapley.powerwork.R;
import com.smapley.powerwork.fragment.Team;
import com.smapley.powerwork.holder.Tea_Btn_Holder;
import com.smapley.powerwork.holder.Tea_Mem_Holder;
import com.smapley.powerwork.mode.BaseMode;
import com.smapley.powerwork.mode.Tea_Btn_Mode;
import com.smapley.powerwork.mode.Tea_Mem_Mode;

import java.util.List;
import java.util.zip.Inflater;

/**
 * Created by smapley on 15/10/28.
 */
public class TeamAdapter extends RecyclerView.Adapter<RecyclerView.ViewHolder> {

    private Context context;
    private List<BaseMode> list;
    private LayoutInflater inflater;

    public TeamAdapter(Context context, List<BaseMode> list) {
        inflater = LayoutInflater.from(context);
        this.context = context;
        this.list = list;

    }

    @Override
    public RecyclerView.ViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        View view;
        switch (viewType) {
            case 1:
                view = inflater.inflate(R.layout.adapter_tea_mem, parent, false);
                return new Tea_Mem_Holder(view);
            case 2:
                view = inflater.inflate(R.layout.adapter_tea_btn, parent, false);
                return new Tea_Btn_Holder(view);
        }

        return null;
    }

    @Override
    public void onBindViewHolder(RecyclerView.ViewHolder holder, int position) {
        switch (getItemViewType(position)) {
            case 1:
                ((Tea_Mem_Holder) holder).setData(context, (Tea_Mem_Mode) list.get(position));
                break;
            case 2:
                ((Tea_Btn_Holder) holder).setData((Tea_Btn_Mode) list.get(position));
                break;
        }

    }

    @Override
    public int getItemCount() {
        if (list != null) {
            return list.size();
        } else {
            return 0;
        }
    }

    @Override
    public int getItemViewType(int position) {
        return list.get(position).getType();
    }
}