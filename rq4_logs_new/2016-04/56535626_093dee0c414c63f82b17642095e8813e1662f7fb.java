package com.numan1617.tfltubedepartures.network.model;

import com.google.auto.value.AutoValue;
import com.google.gson.TypeAdapterFactory;

@AutoValue
public abstract class Departure {
  public abstract String lineId();
  public abstract String lineName();
  public abstract String platformName();
  public abstract String destinationName();
  public abstract String towards();
  public abstract String expectedArrival();
  public abstract long timeToStation();

  public static TypeAdapterFactory typeAdapterFactory() {
    return AutoValue_Departure.typeAdapterFactory();
  }
}