package com.example.culturavisual;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.ProgressBar;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import java.util.ArrayList;

public class AdapterEstadisticas extends RecyclerView.Adapter<AdapterEstadisticas.ViewHolder> {

    private Context mContext;
    private ArrayList<CuestionarioObj> cuestionarioList;
    private ArrayList<UsuarioCuestionario> mProgressList;
    private Usuarios loggedUser;

    public AdapterEstadisticas(Context context, Usuarios loggedUser, ArrayList<UsuarioCuestionario> progressList){
        this.mContext = context;
        this.loggedUser = loggedUser;
        this.mProgressList = progressList;
    }

    @NonNull
    @Override
    public ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {

        View view = LayoutInflater.from(mContext).inflate(R.layout.estadisticas_item, parent, false);

        return new ViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull ViewHolder holder, int position) {

        UsuarioCuestionario currentItem = mProgressList.get(position);
        int correct = currentItem.getCorrectAnswers();
        int incorrect = currentItem.getWrongAnswers();
        int total = correct + incorrect;
        holder.estadisticasAnswers.setText(String.valueOf(correct) + " / " + String.valueOf(total));
        holder.progressBarCuestionario.setMax(total);
        holder.progressBarCuestionario.setProgress(correct);
    }

    @Override
    public int getItemCount() {
        return mProgressList.size();
    }

    public class ViewHolder extends RecyclerView.ViewHolder{

        public ImageView imageViewEstadisticas;
        public TextView estadisticasCuestionario;
        public TextView estadisticasAnswers;
        public ProgressBar progressBarCuestionario;

        public ViewHolder(@NonNull View itemView) {
            super(itemView);

            imageViewEstadisticas = itemView.findViewById(R.id.imageViewEstadisticas);
            estadisticasCuestionario = itemView.findViewById(R.id.estadisticasCuestionario);
            estadisticasAnswers = itemView.findViewById(R.id.estadisticasAnswers);
            progressBarCuestionario = itemView.findViewById(R.id.progressBarCuestionario);

        }
    }
}