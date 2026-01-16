package com.evolver.chiron.user.CovidTrackerAdapter;

import android.content.Context;
import android.content.Intent;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.bumptech.glide.Glide;
import com.evolver.chiron.user.ModelCovidTracker.ModelCountriesAll.CountryInfo;
import com.evolver.chiron.R;
import com.evolver.chiron.user.UserCovidTrackerCountryDetailAnalysis;

import java.util.List;

public class CountryAdapter extends RecyclerView.Adapter<CountryAdapter.Holder> {

    Context context;
    List<CountryInfo> countryInfos;

    public CountryAdapter(Context context, List<CountryInfo> countryInfos) {
        this.context = context;
        this.countryInfos = countryInfos;
    }

    @NonNull
    @Override
    public Holder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(context).inflate(R.layout.country_list, parent, false);
        return new Holder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull Holder holder, int position) {

        String flag = countryInfos.get(position).getFlag();
        String countryname = countryInfos.get(position).getCountryname();

        holder.CountryName.setText(countryname);
        Glide.with(context).load(flag).into(holder.CountryFlag);

    }

    @Override
    public int getItemCount() {
        return countryInfos.size();
    }

    class Holder extends RecyclerView.ViewHolder implements View.OnClickListener{

        ImageView CountryFlag;
        TextView CountryName;
        public Holder(@NonNull View itemView)  {
            super(itemView);

            CountryFlag = itemView.findViewById(R.id.CountryFlag);
            CountryName = itemView.findViewById(R.id.CountryName);

            itemView.setOnClickListener(this);
        }

        @Override
        public void onClick(View view) {
            Intent intent = new Intent(context, UserCovidTrackerCountryDetailAnalysis.class);
            intent.putExtra("CountryName",countryInfos.get(getBindingAdapterPosition()).getCountryname());
            context.startActivity(intent);
        }
    }
}