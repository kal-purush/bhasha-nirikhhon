package edu.iastate.cpre.scrabblecheater;

import android.content.Context;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.GridView;
import android.widget.ImageView;

/**
 * Created by mpl on 11/10/2016.
 */
public class ImageAdapter extends BaseAdapter {

    private Context mContext;
    private Integer[] mTiles = new Integer[225];

    public ImageAdapter(Context c) {
        mContext = c;

        for (int i = 0; i < 225; i++) {
            mTiles[i] = R.drawable.tile;
        }
    }

    public int getCount() {
        return mTiles.length;
    }

    public Object getItem(int position) {
        return null;
    }

    public long getItemId(int position) {
        return 0;
    }

    public View getView(int position, View convertView, ViewGroup parent){
        ImageView imageView;

        if (convertView == null) {
            imageView = new ImageView(mContext);
            imageView.setLayoutParams(new GridView.LayoutParams(48, 48));
            imageView.setScaleType(ImageView.ScaleType.CENTER_CROP);
            imageView.setPadding(1, 1, 1, 1);

        } else {
            imageView = (ImageView) convertView;
        }

        imageView.setImageResource(mTiles[position]);
        return imageView;
    }
}