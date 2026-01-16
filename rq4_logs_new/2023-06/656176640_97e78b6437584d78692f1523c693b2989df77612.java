package DataLayer.Adapters;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.example.fooddelivery.R;

import java.util.List;

import DataLayer.AppDataSource;
import DataLayer.Models.MenuItem;
import DataLayer.Models.OrderItem;

public class CartAdapter extends RecyclerView.Adapter<CartAdapter.CartViewHolder>{
    private final List<OrderItem> orderItems;

    public CartAdapter(List<OrderItem> orderItems){
        this.orderItems = orderItems;
    }

    @NonNull
    @Override
    public CartViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.cart_card, parent, false);
        return new CartViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull CartViewHolder holder, int position) {
        OrderItem orderItem = orderItems.get(position);
        MenuItem menuItem = AppDataSource.database.menuItemRepository().getMenuItemById(orderItem.getMenuItemId());

        holder.nameTextView.setText(menuItem.getName());

        String price = String.valueOf(menuItem.getPrice());
        holder.priceTextView.setText(price);

        holder.descriptionTextView.setText(menuItem.getDescription());

        String imageName = menuItem.getImageName();

        int resourceId = holder.imageView.getContext().getResources().getIdentifier(imageName, "drawable", holder.imageView.getContext().getPackageName());

        holder.imageView.setImageResource(resourceId);

        String quantity = String.valueOf(orderItem.getQuantity());
        quantity = "X " + quantity;
        holder.quantityTextView.setText(quantity);
    }

    @Override
    public int getItemCount() {
        return orderItems.size();
    }

    public class CartViewHolder extends RecyclerView.ViewHolder{

        ImageView imageView;
        TextView nameTextView;
        TextView priceTextView;
        TextView descriptionTextView;
        TextView quantityTextView;

        public CartViewHolder(@NonNull View itemView) {
            super(itemView);

            imageView = itemView.findViewById(R.id.cart_item_image);
            nameTextView = itemView.findViewById(R.id.cart_item_name);
            priceTextView = itemView.findViewById(R.id.cart_item_price);
            descriptionTextView = itemView.findViewById(R.id.cart_item_description);
            quantityTextView = itemView.findViewById(R.id.cart_item_quantity);
        }
    }
}