package np.com.naxa.factsnepal.feed.bookmarkedfacts;

import android.arch.lifecycle.Observer;
import android.os.Bundle;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.support.design.widget.NavigationView;
import android.support.v4.view.GravityCompat;
import android.support.v4.widget.DrawerLayout;
import android.support.v7.app.ActionBarDrawerToggle;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.view.MenuItem;

import java.util.ArrayList;
import java.util.List;

import np.com.naxa.factsnepal.R;
import np.com.naxa.factsnepal.common.BaseActivity;
import np.com.naxa.factsnepal.common.ListPaddingDecoration;
import np.com.naxa.factsnepal.feed.Fact;
import np.com.naxa.factsnepal.feed.FactsLocalSource;
import np.com.naxa.factsnepal.feed.list.FactsFeedAdapter;
import np.com.naxa.factsnepal.feed.list.FeedListActivity;
import np.com.naxa.factsnepal.publicpoll.PublicPollActivity;
import np.com.naxa.factsnepal.surveys.SurveyStartActivity;
import np.com.naxa.factsnepal.utils.ActivityUtil;

public class BookmarkedFactsActivity extends BaseActivity implements NavigationView.OnNavigationItemSelectedListener {

    private RecyclerView recyclerViewFeed;
    private FactsFeedAdapter adapter;
    private DrawerLayout drawer;

    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.facts_bookmarked_activity);
        bindUI();
        setupRecyclerView();
        setupToolbar(getString(R.string.toolbar_title_bookmarked));
        setupNavigationBar();


        FactsLocalSource.getINSTANCE().getAllBookmarked()
                .observe(this, new Observer<List<Fact>>() {
                    @Override
                    public void onChanged(@Nullable List<Fact> facts) {
                        adapter.updateList(facts);
                    }
                });
    }

    private void setupNavigationBar() {
        ActionBarDrawerToggle toggle = new ActionBarDrawerToggle(
                this, drawer, toolbar, R.string.navigation_drawer_open, R.string.navigation_drawer_close);
        drawer.addDrawerListener(toggle);
        toggle.syncState();
    }

    private void bindUI() {
        recyclerViewFeed = findViewById(R.id.rv_bookmarked_facts);
        drawer = (DrawerLayout) findViewById(R.id.drawer_layout);
        NavigationView navigationView = (NavigationView) findViewById(R.id.nav_view);
        navigationView.setNavigationItemSelectedListener(this);
    }

    private void setupRecyclerView() {
        adapter = new FactsFeedAdapter(new ArrayList<>(), null);
        LinearLayoutManager layoutManager = new LinearLayoutManager(this);
        recyclerViewFeed.setLayoutManager(layoutManager);
        recyclerViewFeed.addItemDecoration(new ListPaddingDecoration(this));
        recyclerViewFeed.setAdapter(adapter);
    }

    @Override
    public boolean onNavigationItemSelected(@NonNull MenuItem item) {
        // Handle navigation view item clicks here.
        int id = item.getItemId();
        switch (id) {
            case R.id.nav_home:
                ActivityUtil.openActivity(FeedListActivity.class,this);
                break;
            case R.id.nav_public_poll:
                ActivityUtil.openActivity(PublicPollActivity.class, this);
                break;
            case R.id.nav_survey:
                ActivityUtil.openActivity(SurveyStartActivity
                        .class, this);
                break;

        }
        return true;
    }

    private void toggleNavDrawer() {
        if (drawer.isDrawerOpen(GravityCompat.START)) {
            drawer.closeDrawer(GravityCompat.START);
        } else {
            drawer.openDrawer(GravityCompat.START);
        }

    }

}