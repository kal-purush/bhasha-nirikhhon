import { Injectable, EventEmitter } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs/Subject';
import 'rxjs/add/observable/fromPromise';
import 'rxjs/add/operator/map';
import { IPageLayout, IContentBlock } from '../models/page-layout';

@Injectable()
export class DynamicUiService {

    getPageLayouts(): Observable<IPageLayout[]> {
        const subject = new Subject<IPageLayout[]>();
        setTimeout(() => {subject.next(PAGELAYOUTS); subject.complete(); }, 100);
        return subject;
    }

    getPageLayout(name: string): IPageLayout {
        return PAGELAYOUTS.find(pageLayout => pageLayout.name === name);
      }
}

const PAGELAYOUTS: IPageLayout[] = [
    {
        name: 'PageOne',
        def: [
            {
                name: 'ContentBlock1'
            },
            {
                name: 'ContentBlock2'
            }
        ]
    }
];