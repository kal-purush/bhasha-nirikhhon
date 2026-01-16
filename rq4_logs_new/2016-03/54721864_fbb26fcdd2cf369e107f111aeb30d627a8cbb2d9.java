package com.lgcampos.carros.adapter;

import android.content.Context;
import android.support.v7.widget.CardView;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.ProgressBar;
import android.widget.TextView;

import com.lgcampos.carros.R;
import com.lgcampos.carros.domain.Carro;
import com.squareup.picasso.Callback;
import com.squareup.picasso.Picasso;

import java.util.List;

/**
 * TODO qual é o objetivo desta classe?
 *
 * @author Lucas Campos
 * @sinse 1.0.0
 */
public class CarroAdapter extends RecyclerView.Adapter<CarroAdapter.CarrosViewHolder> {

    protected  static final String TAG = "livroandroid";
    private final List<Carro> carros;
    private final Context context;
    private final CarroOnClickListener carroOnClickListener;

    public CarroAdapter(Context context, List<Carro> carros, CarroOnClickListener carroOnClickListener) {
        this.context = context;
        this.carros = carros;
        this.carroOnClickListener = carroOnClickListener;
    }

    @Override
    public CarroAdapter.CarrosViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(context).inflate(R.layout.adapter_carro, parent, false);
        CarrosViewHolder holder = new CarrosViewHolder(view);
        return holder;
    }

    @Override
    public void onBindViewHolder(final CarroAdapter.CarrosViewHolder holder, final int position) {
        Carro carro = carros.get(position);
        holder.nome.setText(carro.nome);
        holder.progress.setVisibility(View.VISIBLE);

        Picasso.with(context).load(carro.urlFoto).fit().into(holder.img, new Callback() {
            @Override
            public void onSuccess() {
                holder.progress.setVisibility(View.GONE);
            }

            @Override
            public void onError() {
                holder.progress.setVisibility(View.GONE);
            }
        });

        if (carroOnClickListener != null) {
            holder.itemView.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    carroOnClickListener.onClickCarro(holder.itemView, position);
                }
            });
        }
    }

    @Override
    public int getItemCount() {
        return carros != null ? carros.size() : 0;
    }

    public static class CarrosViewHolder extends RecyclerView.ViewHolder {
        private final TextView nome;
        private final ImageView img;
        private final ProgressBar progress;
        private final CardView cardView;

        public CarrosViewHolder(View view) {
            super(view);

            nome = (TextView) view.findViewById(R.id.text);
            img = (ImageView) view.findViewById(R.id.img);
            progress = (ProgressBar) view.findViewById(R.id.progress_img);
            cardView = (CardView) view.findViewById(R.id.card_view);
        }
    }
}