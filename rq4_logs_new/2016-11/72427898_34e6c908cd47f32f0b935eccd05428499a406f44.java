package com.hormiga6.androidapipractice.MultiTypeList;

import android.content.Context;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.ListView;
import android.widget.TextView;

import com.hormiga6.androidapipractice.R;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class MultiTypeListActivity extends AppCompatActivity {

    private MyCustomAdapter mAdaptor;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_multi_type_list);
        mAdaptor = new MyCustomAdapter();
        for (int i = 0; i < 50; i++) {
            mAdaptor.addItem("item " + i);
            if (i % 10 == 0) {
                mAdaptor.addSeparator("separator " + i);
            }
        }

        ((ListView) findViewById(R.id.listViewMultiType)).setAdapter(mAdaptor);
    }

    //See http://android.amberfog.com/?p=296
    class MyCustomAdapter extends BaseAdapter {
        private static final int TYPE_ITEM = 0;
        private static final int TYPE_SEPARATOR = 1;
        private static final int TYPE_MAX_COUNT = 2;

        private List<String> mData = new ArrayList<String>();
        private LayoutInflater mInflater;

        private Set<Integer> mSeparatorSet = new TreeSet<Integer>();

        public MyCustomAdapter() {
            mInflater = (LayoutInflater) getSystemService(Context.LAYOUT_INFLATER_SERVICE);
        }

        public void addItem(final String item) {
            mData.add(item);
            notifyDataSetChanged();
        }

        public void addSeparator(final String item) {
            mData.add(item);
            mSeparatorSet.add(mData.size() - 1);
            notifyDataSetChanged();
        }

        @Override
        public int getItemViewType(int position) {
            return mSeparatorSet.contains(position) ? TYPE_SEPARATOR : TYPE_ITEM;
        }

        @Override
        public int getViewTypeCount() {
            return TYPE_MAX_COUNT;
        }

        @Override
        public int getCount() {
            return mData.size();
        }

        @Override
        public Object getItem(int position) {
            return mData.get(position);
        }

        @Override
        public long getItemId(int position) {
            return position;
        }

        @Override
        public View getView(int position, View convertView, ViewGroup parent) {
            ViewHolder holder = null;
            int type = getItemViewType(position);
            String previousText = convertView != null ? ((ViewHolder)convertView.getTag()).textView.getText().toString() : "none";
            System.out.println("getView: " + position + " Original text:" + previousText + " type :" + type);
            if (convertView == null) {
                holder = new ViewHolder();
                switch (type) {
                    case TYPE_ITEM:
                        convertView = mInflater.inflate(R.layout.list_item1, null);
                        holder.textView = (TextView) convertView.findViewById(R.id.textViewItem1);
                        break;
                    case TYPE_SEPARATOR:
                        convertView = mInflater.inflate(R.layout.list_item2, null);
                        holder.textView = (TextView) convertView.findViewById(R.id.textViewItem2Label);
                        break;
                }
                convertView.setTag(holder);
            } else {
                holder = (ViewHolder) convertView.getTag();
            }
            holder.textView.setText(mData.get(position));
            return convertView;
        }
    }

    public static class ViewHolder {
        public TextView textView;
    }
}