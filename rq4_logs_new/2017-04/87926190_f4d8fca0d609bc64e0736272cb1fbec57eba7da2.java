package urop.wifitracker;

import android.content.Context;
import android.net.wifi.ScanResult;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.TextView;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by tavish on 4/11/17.
 */

public class ListAdapter extends ArrayAdapter<ScanResult>{

        public ListAdapter(Context context, List<ScanResult> results)
        {
            super(context, 0, results);
        }
        @Override
        public View getView(int position, View convertView, ViewGroup parent)
        {
            if (convertView == null)
            {
                convertView = LayoutInflater.from(getContext()).inflate(R.layout.list_item, parent, false);
            }
            ScanResult result = getItem(position);
            TextView bssid= (TextView) convertView.findViewById(R.id.bssid);
            TextView strength = (TextView) convertView.findViewById(R.id.strength);
            bssid.setText(result.BSSID);
            strength.setText(String.valueOf(result.level));
            return convertView;
        }
}