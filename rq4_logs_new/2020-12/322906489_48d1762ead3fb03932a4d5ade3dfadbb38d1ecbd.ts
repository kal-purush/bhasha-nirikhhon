import { Injectable } from '@angular/core';
import { AngularFirestore } from '@angular/fire/firestore';
import { map } from 'rxjs/operators';

import { ABOUT_COLLECTION } from '../shared/constants';
import { AboutContent } from './about.model';

@Injectable({
    providedIn: 'root'
})
export class AboutService {

    constructor(private afs: AngularFirestore) { }

    getAbout() {
        return this.afs.collection(ABOUT_COLLECTION)
            .snapshotChanges()
            .pipe(
                map(actions => {
                    return actions.map(a => {
                        return {
                            id: a.payload.doc.id,
                            ...a.payload.doc.data() as AboutContent
                        };
                    });
                })
            );
    }

    updateAbout(docId: string, aboutContent: AboutContent) {
        return this.afs.collection(ABOUT_COLLECTION).doc(docId).set(aboutContent);
    }
}