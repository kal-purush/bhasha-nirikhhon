package sg.edu.np.mad.mad_assg;

import android.annotation.SuppressLint;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.core.content.ContextCompat;
import androidx.recyclerview.widget.RecyclerView;

import com.squareup.picasso.Picasso;

import java.util.ArrayList;

public class CategoryRecyclerViewAdapter extends RecyclerView.Adapter<CategoryRecyclerViewAdapter.ViewHolder> {

    private final ArrayList<CategoryData> categoryDataList;
    private OnItemClickListener onItemClickListener;

    // Constructor
    public CategoryRecyclerViewAdapter(ArrayList<CategoryData> categoryDataList) {
        this.categoryDataList = categoryDataList;
    }

    // ViewHolder class
    public class ViewHolder extends RecyclerView.ViewHolder implements View.OnClickListener {
        ImageView imageView;
        TextView name;

        public ViewHolder(View view) {
            super(view);
            imageView = view.findViewById(R.id.categoryitem_img);
            name = view.findViewById(R.id.categoryitem_text);
            view.setOnClickListener(this);
        }

        @Override
        public void onClick(View v) {
            if (onItemClickListener != null) {
                int position = getAdapterPosition();
                if (position != RecyclerView.NO_POSITION) {
                    onItemClickListener.onItemClick(position, categoryDataList.get(position));
                }
            }
        }
    }

    // onCreateViewHolder
    @NonNull
    @Override
    public ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext()).inflate(R.layout.category, parent, false);
        return new ViewHolder(view);
    }

    @Override
    public void onBindViewHolder(@NonNull ViewHolder holder, int position) {
        CategoryData categoryData = categoryDataList.get(position);
        Picasso.get().load(categoryData.getImageUrl()).into(holder.imageView);
        holder.name.setText(categoryData.getCategory());
        int whiteColor = ContextCompat.getColor(holder.name.getContext(), R.color.white);
        holder.name.setTextColor(whiteColor);
    }

    // getItemCount
    @Override
    public int getItemCount() {
        return categoryDataList.size();
    }

    public void setOnItemClickListener(OnItemClickListener onItemClickListener) {
        this.onItemClickListener = onItemClickListener;
    }

    public interface OnItemClickListener {
        void onItemClick(int position, CategoryData categoryData);
    }
}