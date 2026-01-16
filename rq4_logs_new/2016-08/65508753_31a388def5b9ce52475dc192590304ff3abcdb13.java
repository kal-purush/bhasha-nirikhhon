package br.com.radio.adapter;

import android.annotation.TargetApi;
import android.content.Context;
import android.graphics.drawable.Drawable;
import android.os.Build;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import com.crazyhitty.chdev.ks.rssmanager.RssItem;
import com.squareup.picasso.Picasso;

import java.util.List;

import br.com.radio.R;
import br.com.radio.entity.Person;
import br.com.radio.util.CircleTransform;
import br.com.radio.util.RecyclerViewOnClickListenerHack;


public class PersonAdapter extends RecyclerView.Adapter<PersonAdapter.ViewHolder> {
    private Person[] itemsData;
    private Context mContext;
    private LayoutInflater mLayoutInflater;

    public PersonAdapter(Person[] itemsData , Context context) {
        this.itemsData = itemsData;
        this.mContext = context;
        mLayoutInflater = (LayoutInflater) context.getSystemService(Context.LAYOUT_INFLATER_SERVICE);
    }

    // Create new views (invoked by the layout manager)
    @Override
    public PersonAdapter.ViewHolder onCreateViewHolder(ViewGroup parent,int viewType) {
        View itemLayoutView = mLayoutInflater.inflate(R.layout.item_person, parent, false);


        ViewHolder viewHolder = new ViewHolder(itemLayoutView);
        return viewHolder;
    }

    @Override
    public void onBindViewHolder(ViewHolder viewHolder, int position) {

        viewHolder.txtName.setText(itemsData[position].getName());
        viewHolder.txtDescription.setText(itemsData[position].getDescription());
        Picasso.with(mContext).load(R.drawable.back).transform(new CircleTransform()).into(viewHolder.imgPreview);

    }

     static class ViewHolder extends RecyclerView.ViewHolder {

         TextView txtName;
         TextView txtDescription;
         ImageView imgPreview;

         ViewHolder(View itemLayoutView) {
            super(itemLayoutView);
            txtName = (TextView) itemLayoutView.findViewById(R.id.txtName);
            txtDescription = (TextView) itemLayoutView.findViewById(R.id.txtDescription);
            imgPreview = (ImageView) itemLayoutView.findViewById(R.id.imgPreview);
        }
    }





    // Return the size of your itemsData (invoked by the layout manager)
    @Override
    public int getItemCount() {
        return itemsData.length;
    }
}