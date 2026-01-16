package vn.edu.usth.outlook.adapter;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.cardview.widget.CardView;
import androidx.recyclerview.widget.RecyclerView;

import java.util.List;

import vn.edu.usth.outlook.Email_Sender;
import vn.edu.usth.outlook.R;
import vn.edu.usth.outlook.listener.SelectListener;

public class SentAdapter extends RecyclerView.Adapter<SentAdapter.CustomViewHolder> {
    Context context;
    List<Email_Sender> list;
    private final SelectListener listener;
    public SentAdapter(Context context, List<Email_Sender> list, SelectListener listener) {
        this.context = context;
        this.list = list;
        this.listener = listener;
    }

    @NonNull
    @Override
    public CustomViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        // This is where you inflate the layout (Giving a look to our rows)
        LayoutInflater inflater = LayoutInflater.from(context);
        View view = inflater.inflate(R.layout.item_sent,parent,false);
        return new CustomViewHolder(view, listener);
    }

    @Override
    public void onBindViewHolder(@NonNull CustomViewHolder holder,  int position) {
        // assigning values to the views we created in the recycler_view_row layout file
        // based on the position of the recycler view
        holder.textName.setText(list.get(position).getReceiver());
        holder.textHeadmail.setText(list.get(position).getSubject());
        holder.textContent.setText(list.get(position).getContent());
//        holder.imageView.setImageResource(list.get(position).getImage());
//        holder.textDate.setText(list.get(position).getDate());
    }

    @Override
    public int getItemCount() {
        return list.size();
    }
    public void filterList(List<Email_Sender> filteredList){
        list = filteredList;
        notifyDataSetChanged();

    }

    public static class CustomViewHolder extends RecyclerView.ViewHolder {
        // grabbing the view from our recycler_view_row layout file
        // kinda like in the onCreate method
        public TextView textName, textHeadmail, textContent, textDate;
        public ImageView imageView;
        public CardView cardView;
        public CustomViewHolder(@NonNull View itemView, SelectListener listener) {
            super(itemView);
            textName = itemView.findViewById(R.id.nameSent);
            textHeadmail = itemView.findViewById(R.id.head_emailSent);
            textContent = itemView.findViewById(R.id.contentSent);
            imageView = itemView.findViewById(R.id.imageviewSent);
            cardView = itemView.findViewById(R.id.main_containerSent);
            textDate= itemView.findViewById(R.id.dateSent);

            itemView.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View view) {
                    if (listener != null) {
                        int pos = getAdapterPosition();

                        if (pos != RecyclerView.NO_POSITION) {
                            listener.onItemClicked(pos);
                        }
                    }
                }
            });
        }

    }

}