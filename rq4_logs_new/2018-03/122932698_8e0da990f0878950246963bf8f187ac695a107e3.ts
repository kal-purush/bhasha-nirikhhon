import { IPhotoFetcher } from '.';
import { Observable } from 'rxjs/Observable';
import { ProfileFeed } from '../models';

export abstract class FetcherService implements IPhotoFetcher {
  abstract getUserId(email: string): Observable<string>;

  abstract getUserProfile(userId: string): Observable<ProfileFeed>;
}