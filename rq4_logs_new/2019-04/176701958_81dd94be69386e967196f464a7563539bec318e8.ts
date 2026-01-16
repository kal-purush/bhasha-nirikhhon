import { Content } from "./post.model";
import { Injectable } from "@angular/core";
import { Subject, Observable } from "rxjs";
import { HttpClient, HttpHeaders } from "@angular/common/http";
import { environment } from "../../../../environments/environment";
import { map, tap } from "rxjs/operators";

@Injectable({ providedIn: "root" })
export class PostsService {
  private baseUrl = environment.apiUrl;
  private posts: Content[] = [];
  // private postsUpdated = new Subject<Content[]>();
  // const datum: any;

  constructor(private http: HttpClient) {}

  //   httpOptions = {
  //     headers: new HttpHeaders({ "Content-Type": "application/json" })
  //   };

  private _refreshNeeded$ = new Subject<void>();

  get refreshNeed$() {
    return this._refreshNeeded$;
  }

  getPosts(): Observable<Content[]> {
    const url = `${this.baseUrl}/api/v1/content`;
    console.log(url);
    return this.http.get<Content[]>(url);
  }
  // getPostUpdateListener() {
  //   return this.postsUpdated.asObservable();
  // }

  addPost(content: Content): Observable<Content> {
    // const post: Content = data;
    // this.posts.push(post);
    // this.postsUpdated.next([...this.posts]);
    const url = `${this.baseUrl}/api/v1/content`;
    return this.http.post<Content>(url, content).pipe(
      tap(() => {
        this._refreshNeeded$.next();
      })
    );
  }

  removeContent(id: number) {
    const url = `${this.baseUrl}/api/v1/content/${id}`;
    console.log(url);

    return this.http.delete(url).pipe(
      tap(() => {
        this._refreshNeeded$.next();
      })
    );
  }

  updateContent(id: number, content: Content): Observable<Content> {
    const url = `${this.baseUrl}/api/v1/content/${id}`;
    return this.http.put<Content>(url, content).pipe(
      tap(() => {
        this._refreshNeeded$.next();
      })
    );
  }
}