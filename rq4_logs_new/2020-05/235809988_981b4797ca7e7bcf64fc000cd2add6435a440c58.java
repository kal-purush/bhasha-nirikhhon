package com.group4sweng.scranplan.Social.Messenger;

import android.os.Bundle;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.MenuItem;
import android.view.View;
import android.view.ViewGroup;
import android.widget.SearchView;

import androidx.fragment.app.FragmentTransaction;

import com.google.firebase.firestore.Query;
import com.group4sweng.scranplan.Home;
import com.group4sweng.scranplan.LoadingDialog;
import com.group4sweng.scranplan.R;
import com.group4sweng.scranplan.SearchFunctions.RecipeFragment;
import com.group4sweng.scranplan.SearchFunctions.SearchPrefs;
import com.group4sweng.scranplan.Social.FeedFragment;
import com.group4sweng.scranplan.UserInfo.UserInfoPrivate;

public class MessengerFeedFragment extends FeedFragment {

    final String TAG = "Messenger Feed Fragment";

    View mainView;
    LoadingDialog loadingDialog;

    //Fragment handlers
    private FragmentTransaction fragmentTransaction;
    private RecipeFragment recipeFragment;

    //User information
    private com.group4sweng.scranplan.UserInfo.UserInfoPrivate mUser;
    private SearchPrefs prefs;

    //Menu items
    private SearchView searchView;
    private MenuItem sortButton;

    Query query;


    public MessengerFeedFragment(UserInfoPrivate userSent) {
        super(userSent);
    }

    // Auto-generated onCreate method (everything happens here)
    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        // TODO Remove from here? and make a see all messages view
        View view = inflater.inflate(R.layout.fragment_messenger, container, false);
        mainView = view;
        loadingDialog = new LoadingDialog(getActivity());

        initPageItems(view);
        initPageListeners();

        Home home = (Home) getActivity();
        if (home != null) {
            // Gets search activity from home class and make it invisible
            searchView = home.getSearchView();
            sortButton = home.getSortView();

            sortButton.setVisible(false);
            searchView.setVisibility(View.INVISIBLE);

            //setSearch();

            //Gets search preferences from home class
            prefs = home.getSearchPrefs();
        }

        addPosts(view);

        // Checks users details have been provided
        if(mUser == null){
            // If scroll views fail due to no user, this error is reported
            Log.e(TAG, "ERROR: Loading messenger - We were unable to find user.");
        }
        return view;
    }

    @Override
    protected void addPosts(View view) {
        super.addPosts(view);
    }
}