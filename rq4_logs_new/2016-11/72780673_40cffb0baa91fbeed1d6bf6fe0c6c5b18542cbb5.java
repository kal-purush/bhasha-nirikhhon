package com.flickr.photoapp.flickr;

import android.content.Context;
import android.util.Log;

import com.android.volley.Request;
import com.android.volley.RequestQueue;
import com.android.volley.Response;
import com.android.volley.toolbox.Volley;
import com.flickr.photoapp.json.JacksonRequest;
import com.flickr.photoapp.photos.PhotosContainer;

import rx.Observable;
import rx.Subscriber;

public class FlickrClient {

    private static final String TAG = FlickrClient.class.getSimpleName();

    private static final String FLICKR_SEARCH_TEMPLATE = "https://api.flickr.com/services/rest/?method=flickr.photos.search&api_key=b48131cc891e4b264b368ba3b708c43f&format=json&nojsoncallback=1&text=%s";

    private final RequestQueue requestQueue;

    public FlickrClient(Context context) {

        requestQueue = Volley.newRequestQueue(context);
    }

    public Observable<PhotosContainer> getPhotos(final String text) {
        return Observable.create(new Observable.OnSubscribe<PhotosContainer>() {
            @Override
            public void call(final Subscriber<? super PhotosContainer> subscriber) {
                final Response.Listener responseListener = new Response.Listener<PhotosContainer>() {
                    @Override
                    public void onResponse(PhotosContainer response) {
                        if (!subscriber.isUnsubscribed()) {
                            subscriber.onNext(response);
                            subscriber.onCompleted();
                        }
                    }
                };
                final Response.ErrorListener errorListener = error -> {
                    if (!subscriber.isUnsubscribed()) {
                        subscriber.onError(error);
                        subscriber.onCompleted();
                    }
                };
                final String url = String.format(FLICKR_SEARCH_TEMPLATE, text);
                Log.d(TAG, "requesting url " + url);
                final JacksonRequest<PhotosContainer> request = new JacksonRequest<>(
                        Request.Method.GET, url, null, PhotosContainer.class, responseListener, errorListener);
                requestQueue.add(request);
            }
        });
    }
}