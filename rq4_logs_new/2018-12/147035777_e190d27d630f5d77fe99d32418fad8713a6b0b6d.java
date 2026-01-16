package com.example.vlad.androidapp.Views;

import android.os.Bundle;
import android.support.v4.app.Fragment;
import android.support.v4.app.FragmentActivity;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import com.example.vlad.androidapp.Adapters.ProductAdapter;
import com.example.vlad.androidapp.Contracts.FavoritesContract;
import com.example.vlad.androidapp.Entities.Product;
import com.example.vlad.androidapp.Intractors.GetFavoritesIntractorImpl;
import com.example.vlad.androidapp.Presenters.FavoritesPresenterImpl;
import com.example.vlad.androidapp.R;
import com.example.vlad.androidapp.RecyclerItemClickListener;

import java.util.List;

import butterknife.BindView;
import butterknife.ButterKnife;

public class FavoritesFragment extends Fragment implements FavoritesContract.FavoritesView {
    @BindView(R.id.recyclerView)
    protected RecyclerView recyclerView;

    private FavoritesContract.FavoritesPresenter presenter;
    private ProductAdapter productAdapter;

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
        View view = inflater.inflate(R.layout.favorites_activity, container, false);
        ButterKnife.bind(this, view);
        presenter = new FavoritesPresenterImpl(this, new GetFavoritesIntractorImpl());
        recyclerView.setHasFixedSize(true);
        recyclerView.setLayoutManager(new LinearLayoutManager(getActivity()));
        productAdapter = new ProductAdapter(recyclerItemClickListener);
        presenter.requestDataFromServer();
        return view;
    }

    private RecyclerItemClickListener recyclerItemClickListener = new RecyclerItemClickListener() {
        @Override
        public void onItemClick(Product product) {
            getActivity()
                    .getSupportFragmentManager()
                    .beginTransaction()
                    .replace(R.id.container, new ProductDetailFragment())
                    .addToBackStack(null)
                    .commit();
        }
    };

    @Override
    public void setDataToRecyclerView(List<Product> productsArrayList) {
        productAdapter.setProducts(productsArrayList);
        recyclerView.setAdapter(productAdapter);
    }

    @Override
    public FragmentActivity getNowActivity() {
        return getActivity();
    }

    @Override
    public void onResponseFailure(Throwable throwable) {
        throwable.getCause();
    }

    @Override
    public void onDestroy() {
        super.onDestroy();
        presenter.onDestroy();
    }
}