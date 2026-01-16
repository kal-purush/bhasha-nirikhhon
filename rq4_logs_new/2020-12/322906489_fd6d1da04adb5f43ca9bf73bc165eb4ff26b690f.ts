import { Injectable } from '@angular/core';
import { map } from 'rxjs/operators';
import { AngularFirestore } from '@angular/fire/firestore';
import { BlogSnippet } from './models/blog-snippet.model';

@Injectable({
    providedIn: 'root'
})
export class BlogService {

    constructor(private afs: AngularFirestore) { }

    getBlogSnippets() {
        return this.afs.collection(
                'blog-snippets', ref => ref.orderBy('date', 'desc'))
            .snapshotChanges()
            .pipe(
                map(actions => {
                    return actions.map(a => {
                        return {
                            id: a.payload.doc.id,
                            ...a.payload.doc.data() as BlogSnippet
                        };
                    });
                })
            );
    }

    getBlogPosts() {

    }

    getBlogPostByTitle(title: string) {

    }
}