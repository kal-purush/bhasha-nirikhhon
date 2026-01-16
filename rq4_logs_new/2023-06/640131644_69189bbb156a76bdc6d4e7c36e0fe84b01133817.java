package sg.edu.np.mad.mad_assg;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import java.util.ArrayList;

public class HistoryAdapter extends RecyclerView.Adapter<HistoryViewHolder> {
    ArrayList<String>data;

    public HistoryAdapter(ArrayList<String> input) {
        data = input;
    }

    @NonNull
    @Override
    public HistoryViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        View item = LayoutInflater.from(parent.getContext()).inflate(R.layout.item_history, parent, false);
        return new HistoryViewHolder(item);
    }

    public void onBindViewHolder(HistoryViewHolder holder, int position){
        String s = data.get(position);
        holder.txt.setText(s);
    }

    public int getItemCount(){
        return data.size();
    }
}