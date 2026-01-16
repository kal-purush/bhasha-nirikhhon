import QueueItem from "./Queueitem"
import PromiseState from "./PromiseState"


const INTERNAL = ():void=> { }

class Promit {
	private outcome: any = void 0
	private queue: QueueItem[] = []
	private handled: boolean = false
	state: PromiseState = PromiseState.PENDING

	static resolve(value:any) :Promit {
		if (value instanceof this) {
			return value
		}
		return new Promit(INTERNAL).resolve(value)
	}
	static reject(reason: any):Promit {
		const promise = new this(INTERNAL)
		return promise.reject(reason)
	}
	static all(iterable:Promit[]):Promit {
		if (Object.prototype.toString.call(iterable) !== '[object Array]') {
			return this.reject(new TypeError('must be an array'))
		}
		const len = iterable.length
		let called = false
		if (!len) {
			return this.resolve([])
		}
		const values = new Array(len)
		let resolved = 0
		let i = -1
		const promise = new this(INTERNAL)

		while (++i < len) {
			allResolver(iterable[i], i)
		}
		return promise
		function allResolver(value:any, i:number) :Promit {
			return Promit.resolve(value).then(resolveFromAll, (error:Error):void =>{
				if (!called) {
					called = true
					promise.reject(error)
				}
			})
			function resolveFromAll(outValue:any):void {
				values[i] = outValue
				if (++resolved === len && !called) {
					called = true
					promise.resolve(values)
				}
			}
		}
	}
	static race(iterable:Promit[]):Promit {
		if (Object.prototype.toString.call(iterable) !== '[object Array]') {
			return this.reject(new TypeError('must be an array'))
		}
		const len = iterable.length
		let called = false
		if (!len) {
			return this.resolve([])
		}
		let i = -1
		const promise = new this(INTERNAL)
		while (++i < len) {
			resolver(iterable[i])
		}
		return promise
		function resolver(value:any):Promit {
			return Promit.resolve(value).then(function(response:any):any {
				if (!called) {
					called = true
					promise.resolve(response)
				}
				return null
			}, function(error:Error):void {
				if (!called) {
					called = true
					promise.reject(error)
				}
			})
		}
	}
	constructor(resolver: Function) {
		if (typeof resolver !== 'function') {
			throw new TypeError('resolver must be a function')
		}
		if (resolver !== INTERNAL) {
			this.safelyResolveThenable(resolver)
		}
	}
	private safelyResolveThenable(thenable: Function):void {
		// eslint-disable-next-line @typescript-eslint/no-this-alias
		const self = this
		function onError(e: Error): any {
			self.reject(e)
		}
		function onSuccess(value: any): any {
			self.resolve(value)
		}
		function tryToUnwrap(): void {
			thenable(onSuccess, onError) // 調用構造函數中的函數
		}
		const result: any = self.tryCatch(tryToUnwrap)
		if (result.status === 'error') {
			onError(result.value)
		}
	}
	private tryCatch(func: Function, value?: any): any {
		const out: { value: any; status: string } = {
			value: null,
			status: "",
		}
		try {
			out.value = func(value)
			out.status = 'success'
		} catch (e) {
			out.status = 'error'
			out.value = e
		}
		return out
	}
	finally(cbd: any):Promit {
		if (typeof cbd !== 'function') {
			return this
		}
		cbd = cbd as Function
		function resolve(value:any):Promit {
			function yes() :any {
				return value
			}
			return Promit.resolve(cbd()).then(yes)
		}
		function reject(reason:Error):Promit {
			function no():void {
				throw reason
			}
			return Promit.resolve(cbd()).then(no)
		}
		return this.then(resolve, reject)
	}
	then(onFulfilled?: Function, onRejected?: Function):Promit {
		if (typeof onFulfilled !== 'function' && this.state === PromiseState.FULFILLED || // 參數不對，不作處理，不用管這行
			typeof onRejected !== 'function' && this.state === PromiseState.REJECTED) {
			return this
		}
		onRejected = onRejected as Function
		onFulfilled = onFulfilled as Function
		const promise = new Promit(INTERNAL)
		this.handled = true
		if (this.state === PromiseState.PENDING) {
			this.queue.push(new QueueItem(promise, onFulfilled, onRejected))
			return promise
		}
		const resolver = this.state === PromiseState.FULFILLED ? onFulfilled : onRejected
		promise.unwrap(resolver, this.outcome)	//  unwrap 下一級
		return promise
	}
	catch(onRejected:Function):Promit {
		return this.then(undefined, onRejected)
	}
	toString():string {
		return `Promise {[[PromiseStatus]]: "${this.state}" [[PromiseValue]]: ${this.outcome}}`
	}
	unwrap(func: any, value: any):void {	// 假定value永遠都不會是promise
		// eslint-disable-next-line @typescript-eslint/no-this-alias
		const self = this
		process.nextTick(function(): any {
			let returnValue
			try {
				returnValue = func(value) // 取then裡面的值
			} catch (e) {
				return self.reject(e)
			}
			if (returnValue === self) { // return 了 this?
				self.reject(new TypeError('Cannot resolve promise with itself'))
			} else {
				self.resolve(returnValue)
			}
		})
	}
	private reject(reason: any): Promit {
		if (this.state !== PromiseState.PENDING) {
			return this
		}
		this.state = PromiseState.REJECTED
		this.outcome = reason
		// eslint-disable-next-line @typescript-eslint/no-this-alias
		const self = this
		if (this.handled === false) {
			process.nextTick(function() {
				if (self.handled === false) {
					process.emitWarning('unhandledRejection', 'Warning')
				}
			})
		}
		let i = -1
		const len = this.queue.length
		while (++i < len) {
			if (this.queue[i].onRejected) {
				this.queue[i].callRejected(reason)
			} else {
				this.queue[i].promit.reject(reason)
			}
		}
		return this
	}
	private resolve(value: any): Promit {	// value可能為promise
		if (this.state !== PromiseState.PENDING) {
			return this
		}
		// eslint-disable-next-line @typescript-eslint/no-this-alias
		const self = this
		if (value instanceof Promit) {	// 返回了一個promise
			// eslint-disable-next-line promise/catch-or-return
			process.nextTick(() => {
				return value.then((data: any) => {
					self.resolve(data)
					return null
				}, (reason: any) => {
					self.reject(reason)
					return null
				})
			})
		} else {
			self.state = PromiseState.FULFILLED
			self.outcome = value
			// 上面改變本身狀態，下面進行下一級操作
			let i = -1
			const len = self.queue.length
			while (++i < len) {
				if (self.queue[i].onFulfilled) {
					self.queue[i].callFulfilled(value)
				} else {
					self.queue[i].promit.resolve(value)
				}
			}
		}
		return this
	}
}


export default Promit