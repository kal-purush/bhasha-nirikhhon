package com.example.vlad.androidapp.Contracts;

import android.support.v4.app.FragmentActivity;

import com.example.vlad.androidapp.Entities.Product;

import java.util.List;

public interface FavoritesContract {
    interface FavoritesPresenter {
        void onDestroy();
        void requestDataFromServer();
    }

    interface FavoritesView {
        FragmentActivity getNowActivity();

        void setDataToRecyclerView(List<Product> productsArrayList);
        void onResponseFailure(Throwable throwable);
    }

    interface GetFavoritesProductIntractor{

        interface OnFinishedListener {
            void onFinished(Product product);
            void onFailure(Throwable t);
        }

        void getProduct(int id, FavoritesContract.GetFavoritesProductIntractor.OnFinishedListener onFinishedListener);

    }
}