package com.test.vkphotoviewer;

import android.content.Context;
import android.support.v4.view.PagerAdapter;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.RelativeLayout;
import android.widget.TextView;

import com.squareup.picasso.Picasso;

import java.util.ArrayList;


public class CustomSwipeAdapter extends PagerAdapter{
    private Context ctx;
    private LayoutInflater layoutInflater;
    ArrayList<String> images = new ArrayList<String>();
    ArrayList<String> titles = new ArrayList<String>();
    public CustomSwipeAdapter(Context ctx, ArrayList<String > images, ArrayList<String> titles){
        this.images = images;
        this.titles = titles;
        this.ctx = ctx;
    }

    @Override
    public int getCount() {
        return images.size();
    }

    @Override
    public boolean isViewFromObject(View view, Object object) {
        return (view==(RelativeLayout)object);
    }

    @Override
    public Object instantiateItem(ViewGroup container, int position){
        layoutInflater = (LayoutInflater)ctx.getSystemService(Context.LAYOUT_INFLATER_SERVICE);
        View item_view= layoutInflater.inflate(R.layout.swipe_layout, container,false);
        ImageView imageView = (ImageView)item_view.findViewById(R.id.imageView);
        TextView textView = (TextView)item_view.findViewById(R.id.image_count);
        if (!titles.get(position).isEmpty()) {
            textView.setText(titles.get(position));
        }else{
            textView.setVisibility(View.INVISIBLE);
        }
        Picasso.with(ctx).load(images.get(position)).into(imageView);
        //imageView.setImageResource(image_resources[position]);
        container.addView(item_view);
        return item_view;
    }

    @Override
    public void destroyItem(ViewGroup container, int position, Object object){
        container.removeView((RelativeLayout)object);
    }
}