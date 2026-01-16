import { TrackByFunction } from '@angular/core';

/**
 * Creates trackBy function.
 * @param trackBy trackBy function.
 */
export function createTrackBy<T>(trackBy: TrackByFunction<T>): TrackByFunction<T> {
  return (index, model) => trackBy(index, model);
}


interface WithId {

  readonly id: string | number;
}

/** Creates trackById function. */
export function trackById<T extends WithId>(): TrackByFunction<T> {
  return createTrackBy<T>((_index, { id }) => id);
}

/** Creates trackByIndex function. */
export function trackByIndex<T>(): TrackByFunction<T> {
  return createTrackBy<T>(index => index);
}

/** Creates trackByValue function.  */
export function trackByValue<T>(): TrackByFunction<T> {
  return createTrackBy<T>((_index, value) => value);
}