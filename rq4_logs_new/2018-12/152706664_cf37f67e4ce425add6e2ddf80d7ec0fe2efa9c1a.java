package mobile.project.cekjadwaldokter.adapterRecyclerView;

import android.content.Context;
import android.support.annotation.NonNull;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import com.squareup.picasso.Picasso;

import java.util.List;

import mobile.project.cekjadwaldokter.R;
import mobile.project.cekjadwaldokter.paket.firebase.FirebaseModul;


public class RecyclerAdapter extends RecyclerView.Adapter<RecyclerAdapter.ViewHolder> {

    List<FirebaseModul> list;
    Context context;

    public RecyclerAdapter(List<FirebaseModul> list, Context context) {
        this.list = list;
        this.context = context;
    }

    @NonNull
    @Override
    public ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int i) {
        View view = LayoutInflater.from(context).inflate(R.layout.activity_list_spesialis,parent,false);
        ViewHolder myHoder = new ViewHolder(view);

        return myHoder;
    }

    @Override
    public void onBindViewHolder(@NonNull ViewHolder holder, int position) {

        FirebaseModul mylist = list.get(position);
        holder.spesial.setText(mylist.getSpesialis());
        Picasso.with(context).load(mylist.getImage()).into(holder.imageView);

    }

    class ViewHolder extends RecyclerView.ViewHolder {
        TextView spesial;
        ImageView imageView;


        public ViewHolder(View itemView) {
            super(itemView);
            spesial = (TextView) itemView.findViewById(R.id.Vspesialis);
            imageView = (ImageView) itemView.findViewById(R.id.imV);

        }
    }

    @Override
    public int getItemCount() {
        int arr = 0;

        try{
            if(list.size()==0){

                arr = 0;

            }
            else{

                arr=list.size();
            }



        }catch (Exception e){



        }

        return arr;
    }
}