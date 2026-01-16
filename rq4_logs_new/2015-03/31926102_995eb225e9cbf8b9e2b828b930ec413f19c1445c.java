package co.johnnyli.gamer;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.TextView;

import org.json.JSONArray;
import org.json.JSONObject;

public class GroupJSONAdapter extends BaseAdapter {

    Context mContext;
    LayoutInflater mInflater;
    JSONArray mJSONArray;

    public GroupJSONAdapter(Context context, LayoutInflater inflater) {
        mContext = context;
        mInflater = inflater;
        mJSONArray = new JSONArray();
    }

    @Override
    public int getCount() {
        return mJSONArray.length();
    }

    @Override
    public Object getItem(int position) {
        return mJSONArray.optJSONObject(position);
    }

    @Override
    public long getItemId(int position) {
        return position;
    }

    @Override
    public View getView(int position, View convertView, ViewGroup parent) {
        ViewHolder holder;
        if (convertView == null) {
            convertView = mInflater.inflate(R.layout.group_list_item, null);
            holder = new ViewHolder();
            holder.groupListItem = (TextView) convertView.findViewById(R.id.group_list_item);
            holder.member = (TextView) convertView.findViewById(R.id.member);
            holder.groupDescription = (TextView) convertView.findViewById(R.id.group_description);
            convertView.setTag(holder);
        } else {
            holder = (ViewHolder) convertView.getTag();
        }
        JSONObject jsonObject = (JSONObject) getItem(position);
        String groupName = jsonObject.optString("name");
        holder.groupListItem.setText(groupName);
        holder.member.setVisibility(View.GONE);
        holder.groupDescription.setVisibility(View.GONE);

        return convertView;

    }

    private static class ViewHolder {
        public TextView groupListItem;
        public TextView member;
        public TextView groupDescription;

    }

    public void updateData(JSONArray jsonArray) {
        mJSONArray = jsonArray;
        notifyDataSetChanged();
    }
}