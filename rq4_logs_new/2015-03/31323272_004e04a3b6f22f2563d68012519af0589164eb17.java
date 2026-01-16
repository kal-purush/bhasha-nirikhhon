package jjohnson.yeoman01;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.ImageView;
import android.widget.TextView;

public class customListAdapter extends ArrayAdapter<String> {
    private final Context context;
    private final String[] values;

    public customListAdapter(Context context, String[] values) {
        super(context, android.R.layout.activity_list_item, values);
        this.context = context;
        this.values = values;
    }

    @Override
    public View getView(int position, View convertView, ViewGroup parent) {
        LayoutInflater inflater = (LayoutInflater) context
                .getSystemService(Context.LAYOUT_INFLATER_SERVICE);
        View rowView = inflater.inflate(android.R.layout.activity_list_item, parent, false);
        TextView textView = (TextView) rowView.findViewById(android.R.id.icon);
        ImageView imageView = (ImageView) rowView.findViewById(android.R.id.icon);
        textView.setText(values[position]);
        // change the icon for Windows and iPhone
        String s = values[position];

        return rowView;
    }
} 