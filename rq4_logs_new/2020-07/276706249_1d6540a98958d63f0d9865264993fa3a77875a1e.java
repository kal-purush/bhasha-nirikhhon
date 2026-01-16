package com.dtavana.foodswipe.adapters;

import android.content.Context;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import androidx.annotation.NonNull;
import androidx.recyclerview.widget.RecyclerView;

import com.dtavana.foodswipe.databinding.ItemRestaurantBinding;
import com.dtavana.foodswipe.models.Restaurant;
import com.parse.FindCallback;
import com.parse.ParseException;
import com.parse.ParseQuery;
import com.parse.SaveCallback;

import java.util.List;

public class RestaurantsAdapter extends RecyclerView.Adapter<RestaurantsAdapter.ViewHolder> {
    public static final String TAG = "RestaurantsAdapter";

    Context ctx;
    List<Restaurant> restaurants;
    List<Restaurant> denied;

    public RestaurantsAdapter(Context context, List<Restaurant> restaurants, List<Restaurant> denied) {
        this.ctx = context;
        this.restaurants = restaurants;
        this.denied = denied;
    }

    public void clear() {
        restaurants.clear();
        notifyDataSetChanged();
    }

    @NonNull
    @Override
    public ViewHolder onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        ItemRestaurantBinding binding = ItemRestaurantBinding.inflate(LayoutInflater.from(ctx), parent, false);
        return new ViewHolder(binding);
    }

    @Override
    public void onBindViewHolder(@NonNull ViewHolder holder, int position) {
        Restaurant restaurant = restaurants.get(position);
        holder.bind(restaurant);
    }

    @Override
    public int getItemCount() {
        return restaurants.size();
    }

    class ViewHolder extends RecyclerView.ViewHolder {

        ItemRestaurantBinding binding;

        Restaurant restaurant;

        public ViewHolder(ItemRestaurantBinding binding) {
            super(binding.getRoot());
            this.binding = binding;
        }

        public void bind(final Restaurant restaurant) {
            this.restaurant = restaurant;
            binding.tvName.setText(restaurant.getName());
            binding.tvCuisine.setText(restaurant.getCuisines());
            binding.btnAccept.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View view) {
                    //TODO: Instantly move to final restaurant and cache fragment there to avoid multiple presses of a button
                    cacheRestaurant();
                }
            });
            binding.btnDeny.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View view) {
                    int index = restaurants.indexOf(restaurant);
                    restaurants.remove(restaurant);
                    denied.add(restaurant);
                    notifyItemRemoved(index);
                }
            });
        }

        private void cacheRestaurant() {
            ParseQuery<Restaurant> query = ParseQuery.getQuery(Restaurant.class);
            query.whereEqualTo(Restaurant.KEY_RESTAURANTID, restaurant.getRestaurantId());
            query.findInBackground(new FindCallback<Restaurant>() {
                @Override
                public void done(List<Restaurant> objects, ParseException e) {
                    if(e != null) {
                        Log.e(TAG, "done: Error checking if restaurant already exists", e);
                        return;
                    }
                    if(objects == null || objects.size() < 1) {
                        Log.d(TAG, "done: Could not find existing restaurant, saving new restaurant");
                        restaurant.saveInBackground(new SaveCallback() {
                            @Override
                            public void done(ParseException e) {
                                if(e != null) {
                                    Log.e(TAG, "done: Error occurred while caching chosen restaurant", e);
                                    return;
                                }
                                Log.d(TAG, "done: Cached new restaurant to attend");
                            }
                        });
                    }
                    else {
                        Log.d(TAG, "done: Found existing restaurant to attend, updating visitedCount");
                        Restaurant cachedRestaurant = objects.get(0);
                        cachedRestaurant.increment(Restaurant.KEY_VISITEDCOUNT);
                        cachedRestaurant.saveInBackground(new SaveCallback() {
                            @Override
                            public void done(ParseException e) {
                                if(e != null) {
                                    Log.e(TAG, "done: Error occurred while incrementing visitedCount chosen restaurant", e);
                                    return;
                                }
                                Log.d(TAG, "done: Incremented an existing restaurant visitedCount");
                            }
                        });
                    }
                    //TODO: Show final restaurant fragment
                }
            });

        }
    }
}