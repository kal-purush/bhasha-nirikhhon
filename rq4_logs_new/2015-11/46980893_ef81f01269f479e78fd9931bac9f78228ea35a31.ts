import { Disposable } from '../disposables/disposable';

class DefaultDisposable implements Disposable {
  private disposed: boolean;
  
  constructor(
    private f?: () => void
  ) {
    this.disposed = false;
  }
  
  dispose() {
    if (this.disposed) return;
    if (this.f) this.f();
    this.disposed = true;
  }
}