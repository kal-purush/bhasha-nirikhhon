import {Injectable} from "@angular/core";
import {delay, Observable, of} from "rxjs";
import {OsUsageContent} from "./models/os-usage.content";
import {OsUsageContentItem} from "./models/os-usage-content-item";

@Injectable({
  providedIn: 'root',
})
export class OsUsageApi {
  // TODO: Add backend requests
  public loadPage(page: number): Observable<OsUsageContent> {
    if ( page > 5) page = 5;
    else if (page < 1) page = 1;

    let items: OsUsageContentItem[] = [
      new OsUsageContentItem('В учебном процессе'),
      new OsUsageContentItem('В научном процессе'),
      new OsUsageContentItem('В хозяйственной дейстельности'),
      new OsUsageContentItem('В обеспечении учебного процесса')
    ];

    let content: OsUsageContent = new OsUsageContent(
      5,
      page,
      items
    );

    return of(content).pipe(delay(100))
  }
}