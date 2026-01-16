import { Observable, ConnectableObservable } from 'rxjs';
import { publish, publishBehavior, publishReplay } from 'rxjs/operators';

export function publishStream<T>(stream: Observable<T>) {
  return stream.pipe(publish()) as ConnectableObservable<T>;
}

export function publishBehaviorStream<T>(stream: Observable<T>, value: T) {
  return stream.pipe(publishBehavior(value)) as ConnectableObservable<T>;
}

export function publishReplayStream<T>(
  stream: Observable<T>,
  bufferSize?: number
) {
  return stream.pipe(publishReplay(bufferSize)) as ConnectableObservable<T>;
}