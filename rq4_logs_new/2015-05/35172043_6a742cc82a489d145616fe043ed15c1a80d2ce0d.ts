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

		constructor(maxLength: number) {
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