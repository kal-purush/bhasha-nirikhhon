import { Trip } from '../models/trips';

export const deriveDatesAndCost = (trip: Trip) => {
  let startDate = new Date(8640000000000000);
  let endDate = new Date(-8640000000000000);
  let totalTripCost = 0;

  if (trip.itinerary && trip.itinerary.length > 0) {
    trip.itinerary.forEach(activity => {
      const newStartDate = activity.startDate.toDate();
      const newEndDate = activity.endDate.toDate();

      if (startDate && newStartDate && newStartDate < startDate) {
        startDate = newStartDate;
      }

      if (endDate && newEndDate && newEndDate > endDate) {
        endDate = newEndDate;
      }

      totalTripCost += activity.cost || 0;
    });
  }

  if (
    startDate === new Date(8640000000000000) ||
    endDate === new Date(-8640000000000000)
  ) {
    return {
      ...trip,
      totalCost: totalTripCost,
    } as Trip;
  }

  const duration = endDate.getDate() - startDate.getDate() + 1;

  return {
    ...trip,
    startDate: startDate,
    endDate: endDate,
    duration: duration,
    totalCost: totalTripCost,
  } as Trip;
};