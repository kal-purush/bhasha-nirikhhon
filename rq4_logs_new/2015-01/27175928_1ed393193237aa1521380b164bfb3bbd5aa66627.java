package edu.upc.eetac.dsa.dsaqt1415g4.utroll;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.TextView;

import java.util.ArrayList;

import edu.upc.eetac.dsa.dsaqt1415g4.utroll.api.User;

public class PendingFriendsAdapter extends BaseAdapter {

    private LayoutInflater inflater;

    private static class ViewHolder {
        TextView tvUserUsername;
    }

    private final ArrayList<User> data;

    public PendingFriendsAdapter(Context context, ArrayList<User> data) {
        super();
        inflater = LayoutInflater.from(context);
        this.data = data;
    }

    @Override
    public int getCount() {
        return data.size();
    }

    @Override
    public Object getItem(int position) {
        return data.get(position);
    }

    @Override
    public long getItemId(int position) {
        return ((User) getItem(position)).getGroupid();
    }

    @Override
    public View getView(int position, View convertView, ViewGroup parent) {
        ViewHolder viewHolder = null;
        if (convertView == null) {
            convertView = inflater.inflate(R.layout.list_row_user, null);
            viewHolder = new ViewHolder();
            viewHolder.tvUserUsername = (TextView) convertView
                    .findViewById(R.id.tvUserUsername);
            convertView.setTag(viewHolder);
        } else {
            viewHolder = (ViewHolder) convertView.getTag();
        }
        String username = data.get(position).getUsername();
        viewHolder.tvUserUsername.setText("Usuario: " + username);

        return convertView;
    }
}