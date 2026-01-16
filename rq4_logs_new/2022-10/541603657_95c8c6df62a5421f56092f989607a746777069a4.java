package com.aptech.team5.mysound.Adapter;


import android.content.Context;
import android.content.Intent;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.aptech.team5.mysound.Activity.PlaySongActivity;
import com.aptech.team5.mysound.Model.Song;
import com.aptech.team5.mysound.R;
import com.aptech.team5.mysound.Service.APIService;
import com.aptech.team5.mysound.Service.DataService;
import com.squareup.picasso.Picasso;

import java.util.ArrayList;

import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;

public class SearchSongAdapter extends RecyclerView.Adapter<SearchSongAdapter.ViewHolder>{
    Context context;
    ArrayList<Song> arraysong;

    public SearchSongAdapter(Context context, ArrayList<Song> arraysong) {
        this.context = context;
        this.arraysong = arraysong;
    }

    @NonNull
    @Override
    public ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        LayoutInflater inflater = LayoutInflater.from(parent.getContext());
        View view = inflater.inflate(R.layout.inline_search_song,parent,false);
        return new ViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull ViewHolder holder, int position) {
        Song baihat = arraysong.get(position);
        holder.txtTenbaihat.setText(baihat.getNameSong());
        holder.txtCasi.setText(baihat.getArtist());
        Picasso.with(context).load(baihat.getImageSong()).into(holder.imgbaihat);
    }

    @Override
    public int getItemCount() {
        return arraysong.size();
    }

    public class ViewHolder extends RecyclerView.ViewHolder{
        TextView txtTenbaihat,txtCasi;
        ImageView imgbaihat,imgluotthich;
        public ViewHolder(View itemView){
            super(itemView);
            imgbaihat = itemView.findViewById(R.id.imagebaihat);
            txtTenbaihat = itemView.findViewById(R.id.textviewsearchtenbaihat);
            txtCasi = itemView.findViewById(R.id.textviewsearchcasi);
            imgluotthich = itemView.findViewById(R.id.imagesearchluotthich);
            itemView.setOnClickListener(new View.OnClickListener(){
                @Override
                public void onClick(View view){
                    Intent intent = new Intent(context, PlaySongActivity.class);
                    intent.putExtra("song",arraysong.get(getPosition()));
                    context.startActivity(intent);
                }
            });
            imgluotthich.setOnClickListener(new View.OnClickListener(){
                @Override
                public void onClick(View view) {
                    imgluotthich.setImageResource(R.drawable.heartfull);
                    DataService dataservice = APIService.getService();
                    Call<String> callback = dataservice.updateLikes("1",arraysong.get(getPosition()).getIdSong());
                    callback.enqueue(new Callback<String>() {
                        @Override
                        public void onResponse(Call<String> call, Response<String> response) {
                            String ketqua = response.body();
                            if (ketqua.equals("success")){
                                Toast.makeText(context,"Da thich", Toast.LENGTH_SHORT).show();
                            }else{
                                Toast.makeText(context, "Loi!",Toast.LENGTH_SHORT).show();
                            }
                        }

                        @Override
                        public void onFailure(Call<String> call, Throwable t) {
                            t.getMessage();
                        }
                    });
                    imgluotthich.setEnabled(false);
                }
            });
        }
    }
}