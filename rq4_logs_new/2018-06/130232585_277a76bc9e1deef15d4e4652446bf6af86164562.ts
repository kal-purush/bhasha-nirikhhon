import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable } from 'rxjs';
import { AngularFirestoreCollection, AngularFirestore } from 'angularfire2/firestore';
import { QueryConfig } from '../../models/pagination.entities';
import { ItemList } from '../../models/item.entities';
import * as _ from 'lodash';

/*
  Generated class for the PaginationService provider.

*/
@Injectable()
export class PaginationService {

  // Source data
  private done = new BehaviorSubject(false);
  private loading = new BehaviorSubject(false);
  private data = new BehaviorSubject([]);
  private currentItems: Array<ItemList> = [];

  private query: QueryConfig;

  // Observable data
  itemsData$: Observable<any>;
  stateLoading$: Observable<boolean> = this.loading.asObservable();
  observableDone$: Observable<boolean> = this.done.asObservable();


  constructor(private afs: AngularFirestore) { }

  // Initial query sets options and defines the Observable
  // passing opts will override the defaults
  init(path: string, field: string, opts?: any) {
    this.query = {
      path,
      field,
      limit: 5,
      reverse: true,
      prepend: false,
      ...opts
    }

    const first = this.afs.collection(this.query.path, ref => {
      return ref
              .orderBy(this.query.field, this.query.reverse ? 'desc' : 'asc')
              .limit(this.query.limit);
    })

    this.mapAndUpdate(first);

    // Create the observable array for consumption in components
    this.itemsData$ = this.data.asObservable()
        .scan( (acc, val) => {
          return this.query.prepend ? val.concat(acc) : acc.concat(val);
        });
  }


  // Retrieves additional data from firestore
  more() {
    const cursor = this.getCursor();

    const more = this.afs.collection(this.query.path, ref => {
      return ref
              .orderBy(this.query.field, this.query.reverse ? 'desc' : 'asc')
              .limit(this.query.limit)
              .startAfter(cursor);
    })
    this.mapAndUpdate(more);
  }

  // Retrieves additional data from firestores
  upload() {
    this.done.next(false);
    this.data = new BehaviorSubject([]);
    const upload = this.afs.collection(this.query.path, ref => {
      return ref
      .orderBy(this.query.field, this.query.reverse ? 'desc' : 'asc')
      .limit(this.query.limit)
    })
    this.mapAndUpdate(upload);
  }

  // Determines the doc snapshot to paginate query
  private getCursor() {
    const current = this.data.value;
    if (current.length) {
      return this.query.prepend ? current[0].doc : current[current.length - 1].doc;
    }
    return null;
  }


  // Maps the snapshot to usable format the updates source
  private mapAndUpdate(col: AngularFirestoreCollection<any>) {

    if (this.done.value || this.loading.value) { return };

    // loading
    this.loading.next(true);

    // Map snapshot with doc ref (needed for cursor)
    return col.snapshotChanges()
      .do(arr => {
        let values = arr.map(snap => {
          const data = snap.payload.doc.data();
          const doc = snap.payload.doc;
          return { ...data, doc };
        });

        // If prepending, reverse the batch order
        values = this.query.prepend ? values.reverse() : values;

        // update source with new values, done loading
        // TODO: I have to see a better wai to avoid the duplicate content.
        values.map(item => this.currentItems.push(item as any));
        this.currentItems = _.uniqBy(this.currentItems, 'uuid');
        this.data.next(this.currentItems);
        this.loading.next(false);

        // no more values, mark done
        if (!values.length) {
          this.done.next(true);
        }
    })
    .take(1)
    .subscribe();
  }

}