import { ChangeDetectionStrategy, Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Responsibility, ResponsibilityService } from '../../shared/responsibility.service';
import { CurrentUser, CurrentUserService } from '../../current-user.service';
import { combineLatest, map, Observable } from 'rxjs';
import { pencilSquare } from '../../bootstrap-icons/bootstrap-icons';
import { PageTitleDirective } from '../../page-title/page-title.directive';
import { Contact } from '../../shared/contact.service';
import { ContactComponent } from '../../shared/responsibility/contact/contact.component';
import { RouterLink } from '@angular/router';
import { IconDirective } from '../../icon/icon.directive';
import { LoadingSpinnerComponent } from '../../loading-spinner/loading-spinner.component';
import { ResponsibilityComponent } from '../../shared/responsibility/responsibility.component';

interface ViewModel {
  responsibilities: Array<Responsibility>;
  user: CurrentUser | null;
}

@Component({
  selector: 'ms-responsibilities',
  standalone: true,
  imports: [
    CommonModule,
    PageTitleDirective,
    ContactComponent,
    RouterLink,
    IconDirective,
    LoadingSpinnerComponent,
    ResponsibilityComponent
  ],
  templateUrl: './responsibilities.component.html',
  styleUrls: ['./responsibilities.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class ResponsibilitiesComponent {
  vm$: Observable<ViewModel>;

  icons = {
    edit: pencilSquare
  };

  constructor(
    responsibilityService: ResponsibilityService,
    currentUserService: CurrentUserService
  ) {
    this.vm$ = combineLatest([
      responsibilityService.list(),
      currentUserService.getCurrentUser()
    ]).pipe(map(([responsibilities, user]) => ({ responsibilities, user })));
  }

  trackById(index: number, object: Responsibility | Contact) {
    return object.id;
  }
}