package np.com.naxa.factsnepal.feed.feedv2;

import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.widget.ImageView;

import java.util.ArrayList;
import java.util.List;

import io.reactivex.android.schedulers.AndroidSchedulers;
import io.reactivex.observers.DisposableObserver;
import np.com.naxa.factsnepal.R;
import np.com.naxa.factsnepal.common.ListPaddingDecoration;
import np.com.naxa.factsnepal.feed.Fact;
import np.com.naxa.factsnepal.feed.FactsLocalSource;
import np.com.naxa.factsnepal.feed.list.FactsFeedAdapter;
import np.com.naxa.factsnepal.feed.list.FactsRemoteSource;

public class FactsFeedActivity extends AppCompatActivity implements FactsFeedAdapter.OnFeedCardItemClickListener {

    private FactsFeedAdapter adapter;
    private RecyclerView recyclerViewFeed;
    private DisposableObserver<List<Fact>> factsDisposable;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_facts_feed);
        bindUI();
        setupRecyclerView();
        FactsLocalSource.getINSTANCE().getAll()
                .observe(this, facts -> adapter.updateList(facts));

    }

    private void bindUI() {
        recyclerViewFeed = findViewById(R.id.rv_facts);
    }

    private void setupRecyclerView() {
        adapter = new FactsFeedAdapter(new ArrayList<>(), this);
        LinearLayoutManager layoutManager = new LinearLayoutManager(this);
        recyclerViewFeed.setLayoutManager(layoutManager);
        recyclerViewFeed.addItemDecoration(new ListPaddingDecoration(this));
        recyclerViewFeed.setAdapter(adapter);
    }

    @Override
    public void onCardTap(Fact fact, ImageView imageView) {

    }

    @Override
    public void onBookmarkButtonTap(Fact fact) {

    }

    @Override
    public void onShareButtonTap(Fact fact) {

    }
}