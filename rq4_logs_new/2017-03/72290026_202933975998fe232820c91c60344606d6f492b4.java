package potapeyko.rss.activities;


import android.content.Context;
import android.database.Cursor;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;
import potapeyko.rss.R;

import java.util.Calendar;
import java.util.Date;

/**
 * Created by Дмитрий on 29.03.2017.
 */
public class mySimpleCursorAdapter extends android.support.v4.widget.SimpleCursorAdapter implements View.OnClickListener {
    @lombok.AllArgsConstructor
    private class myTag {
        long feedItemId;
        int flag;
        boolean type;
    }

    private static final String FLAG_VIEWED_ITEM = "1";
    private static final String FLAG_NOT_VIEWED_ITEM = "0";

    public mySimpleCursorAdapter(Context context, int layout, Cursor c, String[] from, int[] to) {
        super(context, layout, c, from, to);
    }

    public mySimpleCursorAdapter(Context context, int layout, Cursor c, String[] from, int[] to, int flags) {
        super(context, layout, c, from, to, flags);
    }

    @Override
    public View getView(int position, View convertView, ViewGroup parent) {
        View row = super.getView(position, convertView, parent);
        TextView tv = (TextView) row.findViewById(R.id.feedItem_list_flag);
        if (tv != null && tv.getText() != null) {
            if (tv.getText().toString().equals(FLAG_VIEWED_ITEM)) {
                row.setBackgroundColor(this.mContext.getResources().getColor(R.color.colorGray));
            } else {
                row.setBackgroundColor(this.mContext.getResources().getColor(R.color.colorWhite));
            }
        }
        return row;
    }

    @Override
    public void bindView(View view, Context context, Cursor cursor) {
        super.bindView(view, context, cursor);
        final View v = view.findViewById(mTo[2]);
        Long date = cursor.getLong(mFrom[2]);
        Date d = new Date(date);
        Calendar c = Calendar.getInstance();
        c.setTime(d);
        String dateText = String.format("%te.%tm.%ty%n", c, c, c);
        setViewText((TextView) v, dateText);
        View im2 = view.findViewById(R.id.myBTN2);
        View im = view.findViewById(R.id.myBTN1);
        im.setTag(new myTag(cursor.getLong(0), cursor.getInt(7), true));
        im2.setTag(new myTag(cursor.getLong(0), cursor.getInt(7), false));
        im.setOnClickListener(this);
        im2.setOnClickListener(this);

    }

    @Override
    public void onClick(View v) {
        myTag tag = (myTag) v.getTag();
        ImageView im = (ImageView) v;
        if (!tag.type) {
                im.setImageResource(R.drawable.ic_item_bookmark_not);
        } else {
                im.setImageResource(R.drawable.ic_item_check_not);
        }
    }
}
