package com.iskcon.pb;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.TextView;

import com.makeramen.roundedimageview.RoundedImageView;
import com.squareup.picasso.Picasso;

import java.io.File;
import java.util.ArrayList;

/**
 * Created by maygupta on 7/4/15.
 */
public class MediaAdapter extends ArrayAdapter<KirtanData> {

    private final Context context;
    private final ArrayList<KirtanData> values;
    private File mDir;

    public MediaAdapter(Context context, ArrayList<KirtanData> data, File dir) {
        super(context, -1, data);
        this.context = context;
        this.values = data;
        mDir = dir;
    }


    @Override
    public View getView(int position, View convertView, ViewGroup parent) {

        if (convertView == null) {
            LayoutInflater inflater = (LayoutInflater) context
                    .getSystemService(Context.LAYOUT_INFLATER_SERVICE);

            convertView = inflater.inflate(R.layout.media_layout, parent, false);
        }
        TextView textView = (TextView) convertView.findViewById(R.id.song_name);
        TextView tvSongType = (TextView) convertView.findViewById(R.id.tvSongType);

        tvSongType.setText(values.get(position).getmName());
        textView.setText(values.get(position).author);


        RoundedImageView ivAuthorImage = (RoundedImageView) convertView.findViewById(R.id.ivAuthorImage);
        ivAuthorImage.setImageResource(0);
        Picasso.with(getContext()).load(getImageUrl(values.get(position).author)).into(ivAuthorImage);

        convertView.setTag(values.get(position).getmUrl());
        File file = new File(mDir+"/iskcon/"+ values.get(position).getmName());

        // Check if the Music file already exists
        if (file.exists()) {
            convertView.findViewById(R.id.download_icon).setVisibility(View.GONE);
            convertView.findViewById(R.id.play_icon).setVisibility(View.VISIBLE);
        }

        return convertView;

    }

    public String getImageUrl(String author) {
        //return "http://www.iskconpunjabibagh.com/wp-content/uploads/2013/05/HG-Rukmini-Krishna-Prabhu.jpg";
        if ( author.equals("HG Rukmini Krishna Prabhu")) {
            return "http://i59.tinypic.com/65ydj6.png";
        }
        if ( author.equals("HH BM Govind Maharaj")) {
            return "http://iskconleaders.com/wp-content/uploads/2012/03/Bhakti-Madhurya-Govinda-Swami1.jpg";
        }
        if ( author.equals("HH Gopal Krishna Maharaj")) {
            return "http://iskconmiraroad.com/wp-content/uploads/2015/09/VyasaPujaceremonyofGopalKrishnaGoswamiMaharajcelebratedatISKCONDelhi57-140x140.jpg";
        }
        if ( author.equals("Srila Prabhupada")) {
            return "http://www.vina.cc/wp-content/uploads/2015/09/prabhupada-portrait.jpg";
        }
        if ( author.equals("HH BBGS")) {
            return "http://gbc.iskcon.org/wp-content/uploads/2011/12/bbgs_2009_may-300x300.jpg";
        }
        if ( author.equals("HH Loknath Swami")) {
            return "http://iskconleaders.com/wp-content/uploads/2012/02/Lokanath-Swami1.jpg";
        }
        if ( author.equals("HH Bhati Charu Swami")) {
            return "https://pbs.twimg.com/profile_images/440422377/GM_400x400.jpg";
        }


        return "http://i59.tinypic.com/65ydj6.png";
    }

}