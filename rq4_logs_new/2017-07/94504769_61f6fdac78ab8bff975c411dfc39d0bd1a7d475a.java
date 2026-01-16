package com.zxw.dispatch.adapter;

import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import com.jude.easyrecyclerview.adapter.BaseViewHolder;
import com.zxw.data.bean.GroupMessagesBean;
import com.zxw.dispatch.R;


/**
 * Created by wuqiusen on 2017/6/16.
 */

public class GroupMessageViewHolder extends BaseViewHolder<GroupMessagesBean> {
    TextView title;
    private OnGroupMessagesItemClickListener mListener;

    public GroupMessageViewHolder(ViewGroup parent, OnGroupMessagesItemClickListener listener) {
        super(parent, R.layout.item_group_message);
        this.mListener = listener;
        title = $(R.id.tv_title);
    }

    @Override
    public void setData(final GroupMessagesBean data) {
        super.setData(data);
        title.setText(data.getNoticeInfo());
        title.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                mListener.onGroupMessagesItemClick(data.getId());
            }
        });
    }


    public interface OnGroupMessagesItemClickListener {
        void onGroupMessagesItemClick(int id);
    }
}