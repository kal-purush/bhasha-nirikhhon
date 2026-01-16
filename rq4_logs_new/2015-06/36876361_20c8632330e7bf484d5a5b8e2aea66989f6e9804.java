package com.whcs_ucf.whcs_android;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.TextView;

import java.util.List;

/**
 * Created by Jimmy on 6/17/2015.
 */
public class ControlModuleAdapter extends ArrayAdapter<ControlModule>{
    public ControlModuleAdapter(Context context, int resource) {
        super(context, resource);
    }

    public ControlModuleAdapter(Context context, int resource, int textViewResourceId) {
        super(context, resource, textViewResourceId);
    }

    public ControlModuleAdapter(Context context, int resource, ControlModule[] objects) {
        super(context, resource, objects);
    }

    public ControlModuleAdapter(Context context, int resource, int textViewResourceId, ControlModule[] objects) {
        super(context, resource, textViewResourceId, objects);
    }

    public ControlModuleAdapter(Context context, int resource, List<ControlModule> objects) {
        super(context, resource, objects);
    }

    public ControlModuleAdapter(Context context, int resource, int textViewResourceId, List<ControlModule> objects) {
        super(context, resource, textViewResourceId, objects);
    }

    @Override
    public View getView(int position, View convertView, ViewGroup parent) {

        View v = convertView;

        if (v == null) {
            LayoutInflater vi;
            vi = LayoutInflater.from(getContext());
            v = vi.inflate(R.layout.control_module_list_row, null);
        }

        ControlModule controlModule = getItem(position);

        if (controlModule != null) {
            TextView text1 = (TextView) v.findViewById(R.id.cmText1);

            if (text1 != null) {
                text1.setText(controlModule.getName());
            }
        }

        return v;
    }
}