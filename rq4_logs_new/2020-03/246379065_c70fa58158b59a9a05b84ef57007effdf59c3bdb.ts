import { Injectable } from '@angular/core';
import { createClient } from 'contentful';
import { from } from 'rxjs';
import { map, pluck} from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class ContentfulService {
  private CONFIG = {
    space: 'yp0ih1ugi4ck',
    accessToken: 'AfgrzEkwf8NEqmOTkqNMpGFZi_B7dWtw6ADcxkXDIeA',
    contentTypeIds: {
      contentId: 'data'
    }
  };
  private cdaClient = createClient({
    space: this.CONFIG.space,
    accessToken: this.CONFIG.accessToken,
    environment: "info",
  });

  constructor() {
    // this.getPosts();
  }

  getPosts(query?: object): any {
    return from(
      this.cdaClient.getEntries({
        ...Object,
        content_type: this.CONFIG.contentTypeIds.contentId,
        query
      })
    ).pipe(map(posts => posts.items));
  }

  getDataById(id: string): any {
    return from(this.cdaClient.getEntry(id)).pipe(pluck('fields'),pluck('data'));
  }
}