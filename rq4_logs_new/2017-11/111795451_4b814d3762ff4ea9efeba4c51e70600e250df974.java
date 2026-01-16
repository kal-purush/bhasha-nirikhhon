package nova.samplerecyclerview;

import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import java.util.ArrayList;

/**
 * Created by Administrator on 2017-11-23.
 */

public class ListAdapter extends RecyclerView.Adapter{

    ArrayList<ShoppingList> data;

    ListAdapter(ArrayList<ShoppingList> data){
        this.data = data;
    }

    @Override
    public RecyclerView.ViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        View item = LayoutInflater.from(parent.getContext()).inflate(
                R.layout.item, parent, false);
        return new ListItemViewHolder(item);
    }

    @Override
    public void onBindViewHolder(RecyclerView.ViewHolder holder, int position) {
        ShoppingList shoppingList = data.get(position);
        ListItemViewHolder item = (ListItemViewHolder) holder;
        item.date.setText(shoppingList.date);
        item.cal.setText(shoppingList.cal);
        item.salt.setText(shoppingList.salt);
        item.sugar.setText(shoppingList.sugar);
    }

    @Override
    public int getItemCount() {
        return data.size();
    }
}