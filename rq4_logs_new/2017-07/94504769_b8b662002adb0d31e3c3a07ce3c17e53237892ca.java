package com.zxw.dispatch.adapter;

import android.content.Context;
import android.support.v7.widget.RecyclerView;
import android.text.TextUtils;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;

import com.zxw.data.bean.ContactInfo;
import com.zxw.dispatch.R;

import java.util.List;

import butterknife.Bind;
import butterknife.ButterKnife;

/**
 * author：CangJie on 2016/11/7 09:48
 * email：cangjie2016@gmail.com
 */
public class ContactInfoAdapter extends RecyclerView.Adapter<ContactInfoAdapter.ViewHolder> {

    private final Context mContext;
    private final LayoutInflater mLayoutInflater;
    private final List<ContactInfo> mData;
    private final OnClickContactListener mListener;


    public ContactInfoAdapter(Context context, List<ContactInfo> contactInfos, OnClickContactListener listener) {
        this.mContext = context;
        mLayoutInflater = LayoutInflater.from(context);
        this.mData = contactInfos;
        this.mListener = listener;
    }

    @Override
    public ViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        View inflate = mLayoutInflater.inflate(R.layout.item_contact_container, parent, false);
        return new ViewHolder(inflate);
    }

    @Override
    public void onBindViewHolder(ViewHolder holder, final int position) {
        holder.mBtn.setText(mData.get(position).getName());
        if (TextUtils.isEmpty(mData.get(position).getUserId())){
            holder.mBtn.setEnabled(false);
        }else{
            holder.mBtn.setEnabled(true);
        }
        holder.mBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                mListener.onClickListener(mData.get(position).getUserId());
            }
        });
    }

    @Override
    public int getItemCount() {
        return mData.size();
    }

    public class ViewHolder extends RecyclerView.ViewHolder{
        @Bind(R.id.btn_contact)
        Button mBtn;

        public ViewHolder(View itemView) {
            super(itemView);
            ButterKnife.bind(this, itemView);
        }
    }

    public interface OnClickContactListener{
        void onClickListener(String rongId);
    }
}