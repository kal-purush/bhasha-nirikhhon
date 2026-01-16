package com.tecnm.campusuruapan.formutecfisica.Adaptadores;

import android.annotation.SuppressLint;
import android.content.Intent;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ArrayAdapter;
import android.widget.ImageView;
import android.widget.ListView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.constraintlayout.widget.ConstraintLayout;
import androidx.recyclerview.widget.RecyclerView;

import com.tecnm.campusuruapan.formutecfisica.Formulas;
import com.tecnm.campusuruapan.formutecfisica.Modelos.Tema;
import com.tecnm.campusuruapan.formutecfisica.R;

import java.util.List;

public class AdaptadorTema extends RecyclerView.Adapter<AdaptadorTema.TemaViewHolder> {
    private List<Tema> listaTemas;

    public AdaptadorTema(List<Tema> listaTemas) {
        this.listaTemas = listaTemas;
    }

    @NonNull
    @Override
    public TemaViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.menu_formula,parent,false);
        return  new TemaViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull TemaViewHolder holder, int position) {
        holder.textView_Tema.setText(listaTemas.get(position).getTema());
        holder.imageView_ImagenTema.setImageResource(listaTemas.get(position).getImagen());
        boolean expanded = listaTemas.get(position).isExpandido();
        holder.constraintLayout_expandible_layout.setVisibility(expanded?View.VISIBLE:View.GONE);

        ArrayAdapter<String> adapter = new ArrayAdapter<>(holder.view.getContext(),android.R.layout.simple_list_item_1,listaTemas.get(position).getSubtemas());
        holder.listView_ListaSubtemas.setAdapter(adapter);
    }

    @Override
    public int getItemCount() {
        return listaTemas.size();
    }

    class TemaViewHolder extends RecyclerView.ViewHolder
    {
        ConstraintLayout constraintLayout_container;
        ImageView imageView_ImagenTema;
        TextView textView_Tema;
        ConstraintLayout constraintLayout_expandible_layout;
        ListView listView_ListaSubtemas;
        View view;

        @SuppressLint("ClickableViewAccessibility")
        TemaViewHolder(@NonNull View itemView) {
            super(itemView);
            constraintLayout_container = itemView.findViewById(R.id.constraintLayout_container);
            imageView_ImagenTema = itemView.findViewById(R.id.imageView_ImagenTema);
            textView_Tema = itemView.findViewById(R.id.TextView_Tema);
            constraintLayout_expandible_layout = itemView.findViewById(R.id.constraintLayout_expandible_layout);
            listView_ListaSubtemas = itemView.findViewById(R.id.listView_ListaSubtemas);
            view=itemView;

            constraintLayout_container.setOnClickListener(view -> {
                Tema tema = listaTemas.get(getAdapterPosition());
                tema.setExpandido(!tema.isExpandido());
                notifyItemChanged(getAdapterPosition());
            });

            listView_ListaSubtemas.setOnItemClickListener((adapterView, view2, i, l) -> {
                String nombreTema = itemView.getContext().getResources().getString(listaTemas.get(getAdapterPosition()).getTema());
                Intent intent = new Intent(itemView.getContext(), Formulas.class);
                intent.putExtra("nombreTema",nombreTema);
                intent.putExtra("tema",getAdapterPosition());
                intent.putExtra("subtema",i);
                itemView.getContext().startActivity(intent);

            });

            // Setting on Touch Listener for handling the touch inside ScrollView
            listView_ListaSubtemas.setOnTouchListener((v, event) -> {
                // Disallow the touch request for parent scroll on touch of child view
                v.getParent().requestDisallowInterceptTouchEvent(true);
                return false;
            });
        }
    }
}