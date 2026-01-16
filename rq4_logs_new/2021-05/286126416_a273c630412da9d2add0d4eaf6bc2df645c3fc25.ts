import { catchError, finalize, takeUntil } from 'rxjs/operators';
import { ApiService } from '../../../../core/api.service';
import { DataSource } from '@angular/cdk/collections';
import { BehaviorSubject, Observable, of, Subject } from 'rxjs';
import {
  BookProfile,
  BookProfileDTO,
  DestinationType,
} from 'src/app/interfaces';
import { Injectable } from '@angular/core';

interface BookProps {
  author: string;
  category: string;
  description: string;
  title: string;
  destinationType: DestinationType;
  tradingPreferenceAuthor: string;
  tradingPreferenceBook: string;
  tradingPreferenceDescription: string;
  tradingPreferenceGenre: string;
  courier: boolean;
  onFoot: boolean;
  image: File;
}

@Injectable({
  providedIn: 'root',
})
export class BooksListDatasource implements DataSource<any> {
  private unsubscribe = new Subject<void>();
  private readonly initialDataSubject = new BehaviorSubject<BookProfile[]>([]);

  private dataSubject = new BehaviorSubject<BookProfile[]>([]);
  data$ = this.dataSubject.asObservable();

  private loadingSubject = new BehaviorSubject<boolean>(false);
  loading$ = this.loadingSubject.asObservable();

  private noDataSubject = new BehaviorSubject<boolean>(false);
  noData$ = this.noDataSubject.asObservable();

  private countSubject = new BehaviorSubject<number>(0);
  counter$ = this.countSubject.asObservable();

  constructor(private apiService: ApiService) {}

  get books(): BookProfile[] {
    return this.dataSubject.getValue();
  }

  get initialBooks(): BookProfile[] {
    return this.initialDataSubject.getValue();
  }

  getBooksForTable(): void {
    this.loadingSubject.next(true);
    this.noDataSubject.next(false);
    this.apiService
      .fetchBooks(false)
      .pipe(
        takeUntil(this.unsubscribe),
        catchError(() => of([])),
        finalize(() => this.loadingSubject.next(false))
      )
      .subscribe((books: BookProfileDTO[]) => {
        if (!books) return;
        this.noDataSubject.next(!books.length);
        let lineNumber = 0;
        const bookData = books.map((book) => {
          lineNumber = lineNumber + 1;
          return {
            ...book,
            lineNumber,
          };
        });
        this.initialDataSubject.next(JSON.parse(JSON.stringify(bookData)));
        this.dataSubject.next(JSON.parse(JSON.stringify(bookData)));
      });
  }

  async addBook(props: BookProps): Promise<boolean> {
    return new Promise((resolve) => {
      const book = new FormData();
      Object.keys(props).forEach((key: string) => {
        book.append(key, props[key]);
      });
      this.apiService
        .postBookHttp(book)
        .pipe(takeUntil(this.unsubscribe))
        .subscribe(({ newBook }) => {
          if (!newBook) return;
          this.dataSubject.next([
            ...this.books,
            <BookProfile>{
              ...newBook,
              changed: false,
            },
          ]);
          resolve(true);
        });
    });
  }

  updateBooks = (): void => {
    const booksToUpdate = this.books.filter((book) => book.changed);
    booksToUpdate.length &&
      this.apiService
        .putBooksHttp(booksToUpdate)
        .pipe(takeUntil(this.unsubscribe))
        .subscribe(() => {
          this.getBooksForTable();
        });
  };

  deleteBook = (id: string): void => {
    this.apiService
      .deleteBooksHttp(id)
      .pipe(takeUntil(this.unsubscribe))
      .subscribe(() => {
        this.getBooksForTable();
      });
  };

  resetChangedStatus = () => {
    this.dataSubject.next(JSON.parse(JSON.stringify(this.initialBooks)));
  };

  connect(): Observable<BookProfile[] | readonly BookProfile[]> {
    return this.dataSubject.asObservable();
  }

  disconnect(): void {
    this.unsubscribe.next();
    this.unsubscribe.complete();
  }
}