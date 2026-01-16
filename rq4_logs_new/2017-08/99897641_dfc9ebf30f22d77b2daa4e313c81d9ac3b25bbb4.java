package com.exscan.online.exscan;

import android.app.Activity;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.TextView;

import java.util.List;

/**
 * Created by sreed on 8/11/2017.
 */

public class FileExtListAdapter extends BaseAdapter {
    List<FileExtModel> fileExtModels;
    Activity ctx;
    FileExtListAdapter(Activity ctx, List<FileExtModel> files) {
        this.ctx = ctx;
        fileExtModels = files;
    }
    public void update(List<FileExtModel> afiles) {
        fileExtModels = afiles;
        notifyDataSetChanged();
    }
    @Override
    public int getCount() {
        return fileExtModels.size();
    }

    @Override
    public Object getItem(int position) {
        return fileExtModels.get(position);
    }

    @Override
    public long getItemId(int position) {
        return 0;
    }

    @Override
    public View getView(int position, View convertView, ViewGroup parent) {
        View rootView = convertView;
        if (rootView == null) {
            LayoutInflater inflater = ctx.getLayoutInflater();
            rootView = inflater.inflate(R.layout.file_ext_item, null);
            FileExtListAdapter.ViewHolder vh = new FileExtListAdapter.ViewHolder();
            vh.fileext = (TextView) rootView.findViewById(R.id.fileext);
            vh.feq = (TextView) rootView.findViewById(R.id.feq);
            rootView.setTag(vh);
        }
        FileExtListAdapter.ViewHolder vh = (FileExtListAdapter.ViewHolder) rootView.getTag();
        vh.fileext.setText(fileExtModels.get(position).getExtName());
        vh.feq.setText("Frequency "+fileExtModels.get(position).getFrequency());
        return rootView;
    }

    static class ViewHolder {
        public TextView fileext;
        public TextView feq;
    }
}