package ca.huynhat.itsasteal.ui;

import android.os.Bundle;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.support.v4.app.Fragment;
import android.support.v7.widget.DefaultItemAnimator;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import ca.huynhat.itsasteal.R;
import ca.huynhat.itsasteal.utils.DealCommentAdapter;

public class Fragment_Deal_Detail extends Fragment {

    private RecyclerView commentRecyclerList;
    private DealCommentAdapter dealCommentAdapter;

    @Override
    public void onActivityCreated(@Nullable Bundle savedInstanceState) {
        super.onActivityCreated(savedInstanceState);
    }

    @Nullable
    @Override
    public View onCreateView(@NonNull LayoutInflater inflater, @Nullable ViewGroup container, @Nullable Bundle savedInstanceState) {
        View rootView= inflater.inflate(R.layout.deal_detail_view, container,false);

        initCommentList(rootView);

        return rootView;
    }

    private void initCommentList(View view) {

        commentRecyclerList = (RecyclerView) view.findViewById(R.id.detailsView_recycler_comments);
        commentRecyclerList.setLayoutManager(new LinearLayoutManager(getContext()));
        commentRecyclerList.setItemAnimator(new DefaultItemAnimator());

        dealCommentAdapter = new DealCommentAdapter();
        commentRecyclerList.setAdapter(dealCommentAdapter);


    }
}