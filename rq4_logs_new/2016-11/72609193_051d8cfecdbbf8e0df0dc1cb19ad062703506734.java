package news.zhuoxin.com.news.adapter;

import android.content.Context;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import news.zhuoxin.com.news.R;
import news.zhuoxin.com.news.entity.EnterInfo;

/**
 * Created by Administrator on 2016/11/1.
 */

public class EnterAdapter extends MyAdapter<EnterInfo> {

    Context context;

    public EnterAdapter(Context context) {
        super(context);
        this.context = context;
    }

    @Override
    public View setView(int position, View convertView, ViewGroup parent) {
        Holder holder = null;
        if (convertView == null) {
            holder = new Holder();
            convertView = inflater.inflate(R.layout.enter_adapter, parent, false);
            holder.mTxt_time= (TextView) convertView.findViewById(R.id.enter_time);
            holder.mTxt_address= (TextView) convertView.findViewById(R.id.enter_address);
            holder.mTxt_device= (TextView) convertView.findViewById(R.id.enter_device);
            convertView.setTag(holder);
        } else {
            holder = (Holder) convertView.getTag();
        }
        holder.mTxt_time.setText(mList.get(position).getTime());
        holder.mTxt_address.setText(mList.get(position).getAddress());
        holder.mTxt_device.setText(mList.get(position).getDevice());
        return convertView;
    }

    static class Holder {
        TextView mTxt_time;
        TextView mTxt_address;
        TextView mTxt_device;
    }
}