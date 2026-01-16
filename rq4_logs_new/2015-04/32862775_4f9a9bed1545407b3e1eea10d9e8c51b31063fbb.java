package adapter;

import android.content.Context;
import android.graphics.Typeface;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.TextView;

import com.dexteronweb.i_erp.R;

import java.util.ArrayList;

import data.HeadWiseList;

/**
 * Created by kushal on 16/4/15.
 */
public class HeadWiseListAdapter extends BaseAdapter {
    Context context;
    ArrayList<HeadWiseList> headWiseLists;

    public HeadWiseListAdapter(Context context, ArrayList<HeadWiseList> headWiseLists) {
        this.context = context;
        this.headWiseLists = headWiseLists;
    }

    @Override
    public int getCount() {
        return headWiseLists.size();
    }

    @Override
    public Object getItem(int position) {
        return headWiseLists.get(position);
    }

    @Override
    public long getItemId(int position) {
        return headWiseLists.indexOf(position);
    }

    @Override
    public View getView(int position, View convertView, ViewGroup parent) {
        if (convertView == null) {
            LayoutInflater infalInflater = (LayoutInflater) this.context
                    .getSystemService(Context.LAYOUT_INFLATER_SERVICE);
            convertView = infalInflater.inflate(R.layout.head_wise_item, null);
        }

        TextView tvname = (TextView) convertView
                .findViewById(R.id.txtName);
        tvname.setTypeface(null, Typeface.BOLD);
        tvname.setText(headWiseLists.get(position).getProjectName());
        TextView tvwork = (TextView) convertView
                .findViewById(R.id.txtWork);
        tvwork.setTypeface(null, Typeface.BOLD);
        tvwork.setText(headWiseLists.get(position).getWorkDone());
        TextView tvvalue = (TextView) convertView
                .findViewById(R.id.txtValue);
        tvvalue.setTypeface(null, Typeface.BOLD);
        tvvalue.setText(headWiseLists.get(position).getValue());


        return convertView;
    }
}