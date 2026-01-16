package de.picosaan.piasander.skizzenbuchverwaltung;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.ProgressBar;
import android.widget.TextView;

import java.util.ArrayList;

class BuecherAdapter extends ArrayAdapter<Skizzenbuch>{

    Context mContext;

    // View lookup cache
    private static class ViewHolder {
        TextView name;
        ProgressBar progress;
    }

    public BuecherAdapter(ArrayList<Skizzenbuch> buch, Context context) {
        super(context, R.layout.row_item, buch);
        this.mContext=context;
    }

    @Override
    public View getView(int position, View convertView, ViewGroup parent) {
        // Get the data item for this position
        Skizzenbuch buch = getItem(position);
        // Check if an existing view is being reused, otherwise inflate the view
        ViewHolder viewHolder; // view lookup cache stored in tag

        final View result;

        if (convertView == null) {

            viewHolder = new ViewHolder();
            LayoutInflater inflater = LayoutInflater.from(getContext());
            convertView = inflater.inflate(R.layout.row_item, parent, false);
            viewHolder.name = (TextView) convertView.findViewById(R.id.name);
            viewHolder.progress = (ProgressBar) convertView.findViewById(R.id.progress);

            result=convertView;

            convertView.setTag(viewHolder);
        } else {
            viewHolder = (ViewHolder) convertView.getTag();
            result=convertView;
        }

        viewHolder.name.setText(buch.getName());
        viewHolder.progress.setProgress((int)(buch.getAktuelle_seitenzahl() / buch.getSeitenzahl()));
        // Return the completed view to render on screen
        return convertView;
    }
}