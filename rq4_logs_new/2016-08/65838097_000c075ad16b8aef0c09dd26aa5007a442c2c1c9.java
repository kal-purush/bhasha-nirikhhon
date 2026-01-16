package com.reikyz.jandan.adapter;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;

import java.util.List;

/**
 * <p/>
 * 基础的ListView 的 Adapter，实现代码复用
 */
public abstract class BaseListAdapter<T> extends BaseAdapter {

    Context context;
    List<T> models;
    private LayoutInflater inflater;


    public BaseListAdapter(Context context, List<T> models) {
        this.context = context;
        this.models = models;
        inflater = LayoutInflater.from(context);
    }

    /**
     * 判断adapter数据是否为空
     */
    public boolean isEmpty() {
        return models.isEmpty();
    }

    /**
     * 添加加载更多的item
     */
    public void addMoreItem(List<T> items) {
        this.models.addAll(items);
        notifyDataSetChanged();
    }

    /**
     * 添加刷新的Items
     */
    public void refreshItems(List<T> items) {
        models.clear();
        for (int i = 0; i < items.size(); i++) {
            this.models.add(i, items.get(i));
        }
        notifyDataSetChanged();
    }


    @Override
    public int getCount() {
        if (models == null) return 0;
        return models.size();
    }

    @Override
    public T getItem(int position) {
        return models.get(position);
    }

    @Override
    public long getItemId(int position) {
        return position;
    }

    /**
     * 重写该方法，填充item中的数据
     *
     * @param position
     * @param convertView
     * @param parent
     * @return
     */
    @Override
    public abstract View getView(int position, View convertView, ViewGroup parent);


}