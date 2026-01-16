package com.kanyi.bigtuna.adapter;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Adapter;
import android.widget.TextView;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.kanyi.bigtuna.R;
import com.kanyi.bigtuna.models.ModelCartItem;

import java.util.ArrayList;

public class AdapterCartItem extends RecyclerView.Adapter<AdapterCartItem.HolderCartItem>{

    private Context context;
    private ArrayList<ModelCartItem> cartItems;

    public AdapterCartItem(Context context, ArrayList<ModelCartItem> cartItems){
        this.context = context;
        this.cartItems = cartItems;
    }

    @NonNull
    @Override
    public HolderCartItem onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(context).inflate(R.layout.row_cartitem,parent, false);
        return new HolderCartItem(view);
    }

    @Override
    public void onBindViewHolder(@NonNull HolderCartItem holder, int position) {

        ModelCartItem modelCartItem = cartItems.get(position);
        String id = modelCartItem.getId();
        String getpId = modelCartItem.getpId();
        String title = modelCartItem.getName();
        final String cost = modelCartItem.getCost();
        String price = modelCartItem.getPrice();
        String quantity = modelCartItem.getQuantity();

        holder.itemTitleIv.setText(""+title);
        holder.itemPriceIv.setText(""+cost);
        holder.itemQuantityIv.setText("["+quantity+"]");
        holder.itemPriceEachIv.setText(""+price);

        holder.itemRemoveIv.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                EasyDB easyDB = new EasyDB.init(context,"")
                        .setTableName("ITEMS_TABLE")
                        .addColumn(new Column("Item_Id", new String(){"text","unique"}))
                        .addColumn(new Column("Item_PID", new String(){"text","not null"}))
                        .addColumn(new Column("Item_Name", new String(){"text","not null"}))
                        .addColumn(new Column("Item_Price_Each", new String(){"text","not null"}))
                        .addColumn(new Column("Item_Price", new String(){"text","not null"}))
                        .addColumn(new Column("Item_Quantity", new String(){"text","not null"}))
                        .doneTableColumn();

                easyDB.deleteRow(1,id);
                Toast.makeText(context, "Removed from cart...",Toast.LENGTH_SHORT).show();

                cartItems.remove(position);
                notifyItemChanged(position);
                notifyDataSetChanged();
            }
        });
    }

    @Override
    public int getItemCount() {
        return cartItems.size();
    }

    class  HolderCartItem extends RecyclerView.ViewHolder{

        private TextView itemTitleIv, itemPriceIv, itemPriceEachIv, itemQuantityIv, itemRemoveIv;


        public HolderCartItem(@NonNull View itemView) {
            super(itemView);

            itemTitleIv = itemView.findViewById(R.id.itemTitleIv);
            itemPriceIv = itemView.findViewById(R.id.itemPriceIv);
            itemPriceEachIv = itemView.findViewById(R.id.itemPriceEachIv);
            itemQuantityIv = itemView.findViewById(R.id.itemQuantityIv);

        }
    }
}