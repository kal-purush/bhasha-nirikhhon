export type ObservableSubscriptionDone = () => void
export type ObservableSubscription<T> = (item: T, done: ObservableSubscriptionDone) => void

export class Observable<T> {
  protected items: Array<T> = []
  protected subscription: ObservableSubscription<T>
  protected isWaiting = false

  push(item: T) {
    this.items.push(item)
    this.publishNext()
  }

  subscribe(fn: ObservableSubscription<T>) {
    if (this.subscription != null) throw new Error('Already subscribed')
    this.subscription = fn
    this.publishNext()
  }

  protected publishNext() {
    if (this.items.length == 0) return
    if (this.subscription == null) return
    if (this.isWaiting) return

    this.isWaiting = true

    const item = this.items.shift()

    this.subscription(item, () => {
      this.isWaiting = false
      this.publishNext()
    })
  }
}