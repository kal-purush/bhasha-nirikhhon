import { ToastPromiseParams, toast } from "react-toastify";

export class DelayAPI {
	private timeout: number;
	private timeoutID: NodeJS.Timeout|undefined;
	private promise: Promise<void>|undefined;
	private cb: () => Promise<void>;
	private queueCb: ((value: unknown) => void);
	private toastID: string;
	private toastPromiseParams: ToastPromiseParams;

	constructor(timeout: number, toastID: string, toastPromiseParams: ToastPromiseParams) {
		this.timeout = timeout;
		this.queueCb = () => {
			// NOOP
		};
		this.cb = async () => {
			// NOOP
		};
		this.toastID = toastID;
		this.toastPromiseParams = toastPromiseParams;
	}

	queue(cb: () => Promise<void>) {
		clearTimeout(this.timeoutID);
		this.cb = cb;

		if (!this.promise) {
			this.promise = new Promise((resolve) => {
				this.queueCb = resolve;
			}).then(() => this.cb()).then(() => {
				this.promise = undefined;
			});

			toast.promise(
				this.promise,
				this.toastPromiseParams,
				{
					toastId: this.toastID,
				}
			);
		}

		this.timeoutID = setTimeout(this.queueCb, this.timeout);
		return this.promise;
	}
}