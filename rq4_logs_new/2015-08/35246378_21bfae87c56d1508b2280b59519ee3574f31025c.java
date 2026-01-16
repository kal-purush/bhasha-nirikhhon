package github.com.foodactioncommitteecookbook;

import android.support.v4.app.ListFragment;
import android.widget.ListView;

import org.androidannotations.annotations.AfterViews;
import org.androidannotations.annotations.EFragment;
import org.androidannotations.annotations.ViewById;

/**
 *
 */
@EFragment(R.layout.fragment_recipe_list)
public class RecipeListFragment extends ListFragment {

    static RecipeListFragment newInstance() {
        RecipeListFragment fragment = new RecipeListFragment_();

        return fragment;
    }

    @ViewById
    ListView recipeList;

    @AfterViews
    public void init() {

    }
}