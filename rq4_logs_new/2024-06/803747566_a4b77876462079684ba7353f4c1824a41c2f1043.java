package fcu.midterm.bookstore;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.SimpleAdapter;
import android.widget.TextView;

import java.util.List;
import java.util.Map;

public class OrderAdapter extends SimpleAdapter {
    private Context context;
    private List<Map<String, String>> data;
    private int resource;
    private String[] from;
    private int[] to;

    public OrderAdapter(Context context, List<Map<String, String>> data, int resource, String[] from, int[] to) {
        super(context, data, resource, from, to);
        this.context = context;
        this.data = data;
        this.resource = resource;
        this.from = from;
        this.to = to;
    }

    @Override
    public View getView(int position, View convertView, ViewGroup parent) {
        View view = super.getView(position, convertView, parent);

        Button borrowButton = view.findViewById(R.id.btn_borrow);
        TextView bookState = view.findViewById(R.id.order_state);

        // Check book state and disable button if the book is already borrowed
        if ("借閱中".equals(bookState.getText().toString())) {
            borrowButton.setEnabled(false);
        } else {
            borrowButton.setEnabled(true);
        }

        return view;
    }
}
