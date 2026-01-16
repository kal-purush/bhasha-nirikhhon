package sg.edu.np.mad.mad_assg;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.ImageView;
import android.widget.TextView;

import com.squareup.picasso.Picasso;

import java.io.ObjectInputStream;
import java.util.ArrayList;

public class MainRecipeAdapter extends BaseAdapter {

    private LayoutInflater inflater;
    private ArrayList<RecipeList> recipeList;
    private int layout;

    public MainRecipeAdapter(Context context, ArrayList<RecipeList> recipeList, int layout) {
        this.inflater = (LayoutInflater) context.getSystemService(Context.LAYOUT_INFLATER_SERVICE);
        this.recipeList = recipeList;
        this.layout = layout;
    }

    @Override
    public int getCount() {
        return recipeList.size();
    }

    @Override
    public Object getItem(int position) {
        return recipeList.get(position).getRecipeName();
    }

    @Override
    public long getItemId(int position) {
        return position;
    }

    @Override
    public View getView(int position, View convertView, ViewGroup parent) {
        if (convertView == null) {
            convertView = inflater.inflate(layout, parent, false);
        }

        RecipeList recipeItem = recipeList.get(position);
        ImageView icon = (ImageView) convertView.findViewById(R.id.recipe_image_id);
        // Assuming your recipe list is a list of Recipe objects with an image URL property
// Assuming you have a RecipeList object at the desired position
        RecipeList recipe = recipeList.get(position);

// Load the image using Picasso
        Picasso.get().load(recipe.getImageUrl()).into(icon);

        TextView name = (TextView) convertView.findViewById(R.id.recipe_text);
        name.setText(recipeItem.getRecipeName());
        return convertView;
    }
}