package sg.edu.np.mad.mad_assg;

import android.os.Bundle;

import androidx.appcompat.app.AppCompatActivity;
import androidx.recyclerview.widget.DefaultItemAnimator;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import java.util.ArrayList;

public class Recipe extends AppCompatActivity {
    ArrayList<String> myrecipeList = new ArrayList<>();
    @Override
    protected void onCreate(Bundle savedInstanceState){
        super.onCreate(savedInstanceState);
        setContentView(R.layout.recipe);

        myrecipeList.add("Carbonara");
        myrecipeList.add("Briyani");
        myrecipeList.add("Nasi Lemak");
        myrecipeList.add("Chicken Rice");
        myrecipeList.add("Nutella Tarts");

        RecyclerView recyclerView = findViewById(R.id.recipe_recyclerview);
        RecipeAdapter adapter = new RecipeAdapter(myrecipeList);
        LinearLayoutManager mLayoutManager = new LinearLayoutManager(this);
        recyclerView.setLayoutManager(mLayoutManager);
        recyclerView.setItemAnimator(new DefaultItemAnimator());
        recyclerView.setAdapter(adapter);


    }

}