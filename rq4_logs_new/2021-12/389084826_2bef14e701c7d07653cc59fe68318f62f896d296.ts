import { formatName } from './../../../shared/utility';
import { Component, OnInit } from '@angular/core';
import { NgbActiveModal, NgbTypeaheadSelectItemEvent } from '@ng-bootstrap/ng-bootstrap';
import { Observable, of, OperatorFunction } from 'rxjs';
import { catchError, debounceTime, distinctUntilChanged, switchMap, tap } from 'rxjs/operators';
import { AuthService } from '../../auth.service';
import { User } from '../../model/user';

@Component({
  selector: 'app-user-search',
  templateUrl: './user-search.component.html',
  styleUrls: ['./user-search.component.css'],
})
export class UserSearchComponent implements OnInit {
  searching = false;
  searchFailed = false;

  formatName = formatName;

  constructor(private authService: AuthService, private activeModal: NgbActiveModal) {}

  ngOnInit(): void {}

  formatter = (state: User) => state.name_first;

  search: OperatorFunction<string, readonly string[]> = (text$: Observable<string>) =>
    text$.pipe(
      debounceTime(300),
      distinctUntilChanged(),
      tap(() => (this.searching = true)),
      switchMap((term) =>
        this.authService.search(term).pipe(
          tap(() => (this.searchFailed = false)),
          catchError(() => {
            this.searchFailed = true;
            return of([]);
          })
        )
      ),
      tap(() => (this.searching = false))
    );

  selectedItem($event: NgbTypeaheadSelectItemEvent) {
    this.authService.selectedEntryUser.next($event.item);
    this.activeModal.close();
  }
}