import { Eventing } from './Eventing';
import Axios, { AxiosResponse } from 'axios';

export class Collection<T, K> {
  models: T[] = [];
  evnents: Eventing = new Eventing();

  constructor(
    public rootUrl: string,
    public deserialize: (json: K) => T // (parser), get a json string and return an instance of class
  ) {}

  get on() {
    return this.evnents.on;
  }

  get trigger() {
    return this.evnents.trigger;
  }

  fetch(): void {
    Axios.get(this.rootUrl)
      .then((response: AxiosResponse) => {
        response.data.forEach((value: K) => {
          this.models.push(this.deserialize(value));
        });

        this.trigger('change');
      })
      .catch((err) => {
        this.trigger('error');
        throw new Error(err);
      });
  }
}