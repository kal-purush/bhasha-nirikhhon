module streamalicious.core.asyncqueue {
	interface AsyncQueueOperation<T> {
		(callback: Consumer<T>): void;
	}

	interface AsyncQueueJob<T> {
		value: T;
		done: boolean;
		callback: Consumer<T>;
	}

	interface AsyncQueueReadyForMoreCallback {
		(): void;
	}

	export class AsyncQueue<T> {
		private maxLength: number;
		private queue: AsyncQueueJob<T>[] = [];
		private readyForMore: AsyncQueueReadyForMoreCallback = null;

		constructor(maxLength: number = 100) {
			this.maxLength = maxLength;
		}

		private operationDone(job: AsyncQueueJob<T>, value: T) {
			job.done = true;
			job.value = value;

			this.updateQueue();
		}

		private updateQueue() {
			var readyForMore = this.readyForMore;

			while (this.queue.length) {
				if (!this.queue[0].done) {
					break;
				}

				var job = this.queue.shift();
				job.callback(job.value);
			}

			if (this.queue.length < this.maxLength && readyForMore) {
				this.readyForMore = null;
				readyForMore();
			}
		}

		public push(operation: AsyncQueueOperation<T>, callback: Consumer<T>, readyForMore: AsyncQueueReadyForMoreCallback) {
			var job: AsyncQueueJob<T> = {
				value: null,
				done: false,
				callback: callback
			};

			this.queue.push(job);
			operation((value: T) => {
				this.operationDone(job, value);
			});

			if (this.queue.length < this.maxLength) {
				readyForMore();
			} else {
				this.readyForMore = readyForMore;
			}
		}
	}
}

module streamalicious.core {
	export interface Consumer<T> {
		(value: T): void;
	}

	export interface Streamable<T> {
		requestPart(callback: Consumer<T[]>);
	}

	export interface CollectorCollectPartResult<T> {
		done: boolean;
		value?: T;
	}

	export interface Collector<T, U> {
		collectPart(part: T[]): CollectorCollectPartResult<U>;
	}


	export class CoreStream<T> {
		private streamable: Streamable<T>

		constructor(streamable: Streamable<T>) {
			this.streamable = streamable;
		}

		collect<U>(collector: Collector<T, U>, callback: Consumer<U>) {
			// Bootstrap the collecting
			this.collectPart({
				queue: new asyncqueue.AsyncQueue<T[]>(),
				collector: collector,
				callback: callback,
				done: false
			});
		}

		private collectPart<U>(state: {queue: asyncqueue.AsyncQueue<T[]>; collector: Collector<T, U>; callback: Consumer<U>; done: boolean}) {				
			state.queue.push(
				// Operation to do
				(callback: Consumer<T[]>) => {
					this.streamable.requestPart(callback);
				},
				// A value is delivered (these are called in order)
				(part: T[]) => {
					if (!state.done) {
						var result = state.collector.collectPart(part);
						state.done = result.done || !part;
						if (result.done) {
							// Since we are done, lets call the initiator of the collection operation
							state.callback(result.value);
						}
					}
				},
				// What to do when there is more space in the queue
				() => {
					if (!state.done) {
						this.collectPart(state);
					}
				});
		}
	}
}

module streamalicious {
	export class Stream<T> extends core.CoreStream<T> {
		constructor(streamable: core.Streamable<T>) {
			super(streamable);
		}
	}
}

module streamalicious.streams {
	class ArrayStreamable<T> implements core.Streamable<T>{
		private array: T[];

		constructor(array: T[]) {
			this.array = array;
		}

		public requestPart(callback: core.Consumer<T[]>) {
			callback(this.array);
			this.array = null;
		}
	}

	export function fromArray<T>(array: T[]): Stream<T> {
		return new Stream<T>(new ArrayStreamable(array));
	}

	class SleepIncrementerStreamable implements core.Streamable<number>{
		private value: number = 0;

		public requestPart(callback: core.Consumer<number[]>) {
			var currentValue = this.value++;
			if (currentValue > 100) {
				callback(null);
			} else {
				setTimeout(() => {
					callback([currentValue]);
				}, Math.random() * 1000);
			}
		}
	}

	export function debugSleepIncremeneter(): Stream<number> {
		return new Stream<number>(new SleepIncrementerStreamable());
	}
}

module streamalicious.collectors {
	class CountCollector implements streamalicious.core.Collector<number, number> {
	private count: number = 0;

	public collectPart(part: number[]): streamalicious.core.CollectorCollectPartResult<number> {
		if (!part) {
			// I am done...
			return { done: true, value: this.count };
		}

		for (var i = 0, len = part.length; i < len; i++) {
			this.count += part[i];
		}
		
		// Keep going!
		return { done: false };
	}
}
}

var stream = streamalicious.streams.debugSleepIncremeneter();

stream.collect(new CountCollector, (count: number) => {
	console.log("Count is: " + count);
});