import android.content.Context;
import android.content.Intent;
import android.support.annotation.NonNull;
import android.support.v7.widget.RecyclerView;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.LinearLayout;
import android.widget.TextView;
import android.widget.Toast;

import com.example.mohammad.studentpa.R;
import com.example.mohammad.studentpa.TakeSpendingItems;

import java.util.ArrayList;

public class SpendingRecyclerViewAdapter extends RecyclerView.Adapter<SpendingRecyclerViewAdapter.ViewHolder> {
    private static final String TAG = "SpendingRecycvAdapter";

    private ArrayList<String> spendDates = new ArrayList<>();
    private ArrayList<String> spendTotals = new ArrayList<>();
    private Context context;

    public SpendingRecyclerViewAdapter(Context context, ArrayList<String> spendDates, ArrayList<String> spendTotals) {
        this.spendDates = spendDates;
        this.spendTotals = spendTotals;
        this.context = context;
    }

    @NonNull
    @Override
    public SpendingRecyclerViewAdapter.ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view= LayoutInflater.from(parent.getContext())
                .inflate(R.layout.recyclerview_spending_display, parent, false);
        SpendingRecyclerViewAdapter.ViewHolder holder= new SpendingRecyclerViewAdapter.ViewHolder(view);
        return holder;
    }

    @Override
    public void onBindViewHolder(@NonNull ViewHolder holder, final int position) {
        Log.d(TAG, "onBindViewHolder: called.");//log tag

        holder.textViewSpendDate.setText(spendDates.get(position));
        holder.textViewSpendTotal.setText(spendTotals.get(position));

        holder.spendingLayout.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                //On item click, start spending taker activity
                Intent spendIntent= new Intent(context, TakeSpendingItems.class);
                context.startActivity(spendIntent);

                Toast.makeText(context, spendTotals.get(position), Toast.LENGTH_SHORT).show();

            }
        });
    }

    @Override
    public int getItemCount() {
        return spendDates.size();
    }
    public class ViewHolder extends RecyclerView.ViewHolder{

        TextView textViewSpendDate;
        TextView textViewSpendTotal;
        LinearLayout spendingLayout;

        public ViewHolder(View itemView) {
            super(itemView);
            textViewSpendDate = itemView.findViewById(R.id.textViewSpendDate);
            textViewSpendTotal = itemView.findViewById(R.id.textViewSpendTotal);
            spendingLayout = itemView.findViewById(R.id.recycler_spending_display);
        }
    }
}