import { Observable } from 'rxjs/Observable';

export interface IBaseSenderService {
  senderStream: Observable<any>;
}