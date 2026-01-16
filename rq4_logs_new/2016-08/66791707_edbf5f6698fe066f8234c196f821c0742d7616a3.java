package co.minesweepers.maintenance.adapters;

import android.graphics.Color;
import android.support.v7.widget.CardView;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import java.util.Arrays;
import java.util.List;

import co.minesweepers.maintenance.R;

public class RecyclerViewAdapter extends RecyclerView.Adapter<RecyclerViewAdapter.CardsViewHolder> {

    public static final List colors = Arrays.asList(Color.BLUE, Color.RED, Color.GREEN, Color.YELLOW, Color.CYAN);

    @Override
    public CardsViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.main_card_view, parent, false);
        return new CardsViewHolder(view);
    }

    @Override
    public void onBindViewHolder(CardsViewHolder holder, int position) {
        holder.mTitle.setText("Position " + position);
        holder.mCardView.setCardBackgroundColor((int)colors.get(position));
    }

    @Override
    public int getItemCount() {
        return 5;
    }

    public static class CardsViewHolder extends RecyclerView.ViewHolder {

        public CardView mCardView;
        public TextView mTitle;

        public CardsViewHolder(View itemView) {
            super(itemView);
            mCardView = (CardView)itemView.findViewById(R.id.card_view);
            mTitle = (TextView)itemView.findViewById(R.id.info_text);
        }

    }
}