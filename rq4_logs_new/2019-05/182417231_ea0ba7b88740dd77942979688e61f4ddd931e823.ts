import { Observable, Subject } from 'rxjs';

/**
 * Абстрактный контроллер для Subject (потока данных).<br>
 * Создает {@link Subject} и посылает в него данные каждые раз, когда изменяется {@link value}, а так же предоставляет доступ к observable
 * на базе созданного subject.
 */
export class SubjectHandler<T> {

  get value(): T {
    return this._value;
  }

  get observable(): Observable<T> {
    return this._subject.asObservable();
  }

  private _value: T;
  private _subject = new Subject<T>();

  /**
   * @param initValue - первое значение в потоке данных.
   */
  constructor(initValue?: T) {
    this.emit(initValue);
  }

  emit(value: T): void {
    this._value = value;
    this._subject.next(this._value);
  }

  /**
   * Переотправляет в поток данных последнее значение.
   */
  reEmit(): void {
    this._subject.next(this._value);
  }

  /**
   * Завершает поток данных, аналог десктруктора.
   */
  complete(): void {
    this._subject.complete();
    this['completed'] = true;
  }
}