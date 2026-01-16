package io.juismi;

import android.content.Context;
import android.text.Layout;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.view.animation.Animation;
import android.view.animation.AnimationUtils;
import android.widget.ArrayAdapter;
import android.widget.ImageView;
import android.widget.TextView;

import java.util.ArrayList;

/**
 * Created by Ismael on 18/02/2018.
 */

public class CustomViewAdapter extends ArrayAdapter<IssueModel> implements View.OnClickListener {
    private ArrayList<IssueModel> dataSet;
    Context mContext;

    private static class ViewHolder{
        TextView txtName;
        TextView txtNoComments;
        TextView txtStatus;
        ImageView tag;
        ImageView commentIcon;
    }

    public CustomViewAdapter(ArrayList<IssueModel> data, Context context){
        super(context, R.layout.list_issue, data);
        this.dataSet = data;
        this.mContext= context;
    }

    @Override
    public void onClick(View v) {

        int position=(Integer) v.getTag();
        Object object= getItem(position);
        IssueModel dataModel=(IssueModel) object;

        switch (v.getId())
        {

        }
    }

    private int lastPosition = -1;

    public View getView(int position, View convertView, ViewGroup parent){
        IssueModel issueModel = getItem(position);
        ViewHolder viewHolder;

        final View result;

        if(convertView == null){
            viewHolder = new ViewHolder();
            LayoutInflater inflater = LayoutInflater.from(getContext());
            convertView =  inflater.inflate(R.layout.list_issue, parent, false);
            viewHolder.txtName = (TextView) convertView.findViewById(R.id.nameTask);
            viewHolder.txtNoComments = (TextView) convertView.findViewById(R.id.noComments);
            viewHolder.txtStatus = (TextView) convertView.findViewById(R.id.issueStatus);
            viewHolder.tag = (ImageView) convertView.findViewById(R.id.tag);
            //viewHolder.commentIcon = (ImageView) convertView.findViewById(R.id.comment);



            result = convertView;

            convertView.setTag(viewHolder);
        }else{
            viewHolder = (ViewHolder) convertView.getTag();
            result = convertView;
        }
        //Animation animation = AnimationUtils.loadAnimation(mContext, (position > lastPosition) ? R.anim.up_from_bottom : R.anim.down_from_top);
        //result.startAnimation(animation);
        lastPosition = position;

        viewHolder.txtName.setText(issueModel.getName());
        viewHolder.txtNoComments.setText(issueModel.getNoComments());
        viewHolder.txtStatus.setText(issueModel.getStatus());
        viewHolder.tag.setTag(position);
        //viewHolder.commentIcon.setTag(position);

        return convertView;
    }

}