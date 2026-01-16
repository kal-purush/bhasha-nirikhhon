package com.funkyhacker.japanweather;

import android.content.Context;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.ViewGroup;
import com.funkyhacker.japanweather.databinding.ItemForecastListBinding;
import com.funkyhacker.japanweather.model.Forecast;
import java.util.List;

public class ForecastAdapter extends RecyclerView.Adapter<ForecastAdapter.ViewHolder> {

  private Context context;
  private List<Forecast> forecasts;

  public ForecastAdapter(Context context) {
    this.context = context;
  }

  public void setData(List<Forecast> forecasts) {
    this.forecasts = forecasts;
  }

  @Override public ViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
    LayoutInflater inflater = LayoutInflater.from(context);
    ItemForecastListBinding binding = ItemForecastListBinding.inflate(inflater, parent, false);
    return new ViewHolder(binding);
  }

  @Override public void onBindViewHolder(ViewHolder holder, int position) {
    holder.bind(forecasts.get(position));
  }

  @Override public int getItemCount() {
    return forecasts.size();
  }

  public static class ViewHolder extends RecyclerView.ViewHolder {
    private final ItemForecastListBinding binding;

    public ViewHolder(ItemForecastListBinding binding) {
      super(binding.getRoot());
      this.binding = binding;
    }

    public void bind(Forecast forecast) {
      binding.setItem(forecast);
      binding.executePendingBindings();
    }
  }
}