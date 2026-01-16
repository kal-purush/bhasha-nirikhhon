package com.numan1617.tfltubedepartures.feature.stopdetail;

import com.numan1617.tfltubedepartures.network.model.Departure;
import java.util.Comparator;

public class DepartureArrivalComparator implements Comparator<Departure> {
  @Override public int compare(final Departure lhs, final Departure rhs) {
    if (lhs == null && rhs == null) {
      return 0;
    } else if (lhs == null) {
      return 1;
    } else if (rhs == null) {
      return -1;
    } else {
      return Long.compare(lhs.timeToStation(), rhs.timeToStation());
    }
  }
}