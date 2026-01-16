package hr.foi.morder.adapters;

import android.content.Context;
import android.support.annotation.NonNull;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import com.google.firebase.firestore.FirebaseFirestore;

import java.util.List;

import hr.foi.morder.R;
import hr.foi.morder.entities.Artikl;
import hr.foi.morder.entities.Stol;

public class TableRecyclerAdapter extends RecyclerView.Adapter<TableRecyclerAdapter.ViewHolder> {

    private List<Stol> stolList;
    private Context ctx;
    private FirebaseFirestore database;

    public TableRecyclerAdapter(List<Stol> stolList, Context ctx, FirebaseFirestore database){
        this.stolList=stolList;
        this.ctx=ctx;
        this.database=database;
    }

    public TableRecyclerAdapter.ViewHolder onCreateViewHolder(@NonNull ViewGroup viewGroup, int viewType) {
        View view = LayoutInflater.from(viewGroup.getContext()).inflate(R.layout.activity_prikaz_stolova, viewGroup, false);
        return new TableRecyclerAdapter.ViewHolder(view);
    }

    public void onBindViewHolder

    public class ViewHolder extends RecyclerView.ViewHolder {
        public ViewHolder(View view) {
            super();
        }
    }
}