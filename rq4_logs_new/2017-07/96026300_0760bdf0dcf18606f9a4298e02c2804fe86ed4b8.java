package io.happylrd.daphne.activity;

import android.support.design.widget.AppBarLayout;
import android.support.design.widget.CollapsingToolbarLayout;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.support.v7.widget.Toolbar;

import java.util.ArrayList;
import java.util.List;

import butterknife.BindView;
import cn.crepusculo.model.GeneralCardModel;
import cn.crepusculo.model.PureCardModel;
import cn.crepusculo.model.PureCardModelWithAvatar;
import cn.crepusculo.model.RandomData;
import io.happylrd.daphne.R;
import io.happylrd.daphne.adapter.CommentRecyclerAdapter;
import io.happylrd.youbo.common.app.Activity;

/**
 * <h1>modify class name here</h1>
 * io.happylrd.daphne.activity
 * <p>
 * add description here
 *
 * @author Crepusculo
 * @version 1.0
 * @since 2017/7/10
 */

public class TweetActivity extends Activity {
    @BindView(R.id.collapsing_toolbar)
    CollapsingToolbarLayout collapsingToolbarLayout;
    @BindView(R.id.appbar)
    AppBarLayout appBarLayout;
    @BindView(R.id.recycler)
    RecyclerView recyclerView;
    @BindView(R.id.toolbar)
    Toolbar toolbar;

    private String mTitle;

    @Override
    protected int getContentLayout() {
        return R.layout.activity_tweet;
    }

    @Override
    protected void initWidget() {
        super.initWidget();
        initTweet().initCollapsingToolbar().initToolbar().initRecycler();
    }

    @Override
    protected void initData() {
        super.initData();
        this.mTitle = new GeneralCardModel().random().getEventTitle();
        if (getSupportActionBar() != null)
            getSupportActionBar().setTitle(mTitle);
    }

    private TweetActivity initCollapsingToolbar() {
        collapsingToolbarLayout.setTitleEnabled(false);
        collapsingToolbarLayout.setCollapsedTitleTextColor(getResources().getColor(R.color.light_background));
        collapsingToolbarLayout.setTitle("Title");

        appBarLayout.addOnOffsetChangedListener(new AppBarLayout.OnOffsetChangedListener() {
            boolean isShow = false;
            int scrollRange = -1;

            @Override
            public void onOffsetChanged(AppBarLayout appBarLayout, int verticalOffset) {
                if (scrollRange == -1) {
                    scrollRange = appBarLayout.getTotalScrollRange();
                }
                if (scrollRange + verticalOffset == 0) {
                    setToolbarTitle(true);
                    isShow = true;
                } else if (isShow) {
                    isShow = false;
                    setToolbarTitle(false);
                }
            }
        });
        return this;
    }

    private TweetActivity initTweet() {
        return this;
    }

    private TweetActivity initToolbar() {
        setSupportActionBar(toolbar);
        setToolbarTitle(false);
        return this;
    }

    private TweetActivity initRecycler() {
        List<PureCardModelWithAvatar> list = loadDataset();
        recyclerView.setLayoutManager(new LinearLayoutManager(this));
        recyclerView.setAdapter(new CommentRecyclerAdapter<>(list));
        return this;
    }

    private List<PureCardModelWithAvatar> loadDataset() {
        String ss[] = {
                "I used to rule the world",
                "Seas would rise when I gave the word",
                "Now in the morning I sleep alone",
                "Sweep the streets I used to own",

                "I used to roll the dice",
                "Feel the fear in my enemy's eyes",
                "Listened as the crowd would sing,",
                "\"Now the old king is dead! Long live the king!\"",
                "One minute I held the key",
                "Next the walls were closed on me",
                "And I discovered that my castles stand",
                "Upon pillars of salt and pillars of sand",

                "I hear Jerusalem bells a-ringing",
                "Roman cavalry choirs are singing",
                "Be my mirror, my sword and shield",
                "My missionaries in a foreign field",
                "For some reason I can't explain",
                "Once you'd gone there was never",
                "Never an honest word",
                "And that was when I ruled the world",

                "It was a wicked and wild wind",
                "Blew down the doors to let me in",
                "Shattered windows and the sound of drums",
                "People couldn't believe what I'd become",
                "Revolutionaries wait",
                "For my head on a silver plate",
                "Just a puppet on a lonely string",
                "Oh who would ever want to be king?",

                "I hear Jerusalem bells a-ringing",
                "Roman cavalry choirs are singing",
                "Be my mirror, my sword and shield",
                "My missionaries in a foreign field",
                "For some reason I can't explain",
                "I know St. Peter won't call my name",
                "Never an honest word",
                "But that was when I ruled the world",

                "Oh, oh, oh, oh, oh, oh",

                "Hear Jerusalem bells a-ringing",
                "Roman cavalry choirs are singing",
                "Be my mirror, my sword and shield",
                "My missionaries in a foreign field",
                "For some reason I can't explain",
                "I know St. Peter won't call my name",
                "Never an honest word",
                "But that was when I ruled the world"
        };
        List<PureCardModelWithAvatar> list = new ArrayList<>();
        for (String s : ss) {
            GeneralCardModel model = new GeneralCardModel().random();
            list.add((PureCardModelWithAvatar)new PureCardModelWithAvatar()
                    .setPlaceHolderId(R.mipmap.place_holder)
                    .setContent(s)
                    .setAuthor(model.getEventInfo())
                    .setId(model.getId())
            );
        }
        return list;
    }

    private void setToolbarTitle(boolean hide) {
        if (getSupportActionBar() != null)
            if (hide) {
                getSupportActionBar().setTitle("Tweet");
            } else {
                getSupportActionBar().setTitle(mTitle);
            }
    }
}