import { AxiosPromise, AxiosResponse } from 'axios';

type CallbackAlias = () => void; // () =>  {};  !!!! TS  would consider the object as return value !!!!!

interface ModalAttributes<T> {
  get<K extends keyof T>(key: K): T[K];
  getAll(): T;
  set(updateProps: T): void;
}
interface Sync<T> {
  fetch(id: number): AxiosPromise;
  save(data: T): AxiosPromise;
}

interface HasId {
  id?: number;
}

interface Events {
  on(eventName: string, callback: CallbackAlias): void;
  trigger(eventName: string): void;
}
export class Modal<T extends HasId> {
  constructor(
    private attributes: ModalAttributes<T>,
    private sync: Sync<T>,
    private events: Events
  ) {}

  get on() {
    return this.events.on;
  }

  get trigger() {
    return this.events.trigger;
  }

  get get() {
    return this.attributes.get;
  }

  set(updateProps: T): void {
    this.attributes.set(updateProps);
    this.events.trigger('change');
  }

  fetch(): void {
    const id = this.get('id');

    if (typeof id !== 'number') {
      throw new Error('Cannot fetch whiout id');
    }

    this.sync.fetch(id).then((response: AxiosResponse): void => {
      this.set(response.data);
    });
  }

  save(): void {
    this.sync
      .save(this.attributes.getAll())
      .then((response: AxiosResponse): void => {
        this.trigger('save');
      })
      .catch(() => this.trigger('error'));
  }
}