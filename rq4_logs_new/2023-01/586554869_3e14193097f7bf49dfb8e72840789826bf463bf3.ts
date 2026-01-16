import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NonNullableFormBuilder, ReactiveFormsModule, Validators } from '@angular/forms';
import { fileArrowUp, xSquare } from '../../bootstrap-icons/bootstrap-icons';
import { Spinner } from '../../shared/spinner';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { ToastService } from '../../toast/toast.service';
import { first, map, Observable, of, switchMap } from 'rxjs';
import { Activity, ActivityCommand, ActivityService } from '../activity.service';
import { PageTitleDirective } from '../../page-title/page-title.directive';
import { ValidationErrorsComponent } from 'ngx-valdemort';
import { IconDirective } from '../../icon/icon.directive';
import { LoadingSpinnerComponent } from '../../loading-spinner/loading-spinner.component';
import {
  NgbNav,
  NgbNavContent,
  NgbNavItem,
  NgbNavLink,
  NgbNavOutlet
} from '@ng-bootstrap/ng-bootstrap';
import { MarkdownDirective } from '../markdown.directive';

@Component({
  selector: 'ms-activity-edition',
  standalone: true,
  imports: [
    CommonModule,
    PageTitleDirective,
    ReactiveFormsModule,
    ValidationErrorsComponent,
    IconDirective,
    RouterLink,
    LoadingSpinnerComponent,
    NgbNav,
    NgbNavItem,
    NgbNavLink,
    NgbNavContent,
    MarkdownDirective,
    NgbNavOutlet
  ],
  templateUrl: './activity-edition.component.html',
  styleUrls: ['./activity-edition.component.scss']
})
export class ActivityEditionComponent {
  form = this.fb.group({
    title: ['', Validators.required],
    date: this.fb.control<string | null>(null, Validators.required),
    description: ['', Validators.required]
  });
  mode: 'create' | 'edit' | null = null;
  editedActivity?: Activity;
  icons = {
    save: fileArrowUp,
    cancel: xSquare
  };
  saving = new Spinner();

  constructor(
    route: ActivatedRoute,
    private router: Router,
    private activityService: ActivityService,
    private fb: NonNullableFormBuilder,
    private toastService: ToastService
  ) {
    route.paramMap
      .pipe(
        map(paramMap => paramMap.get('activityId')),
        switchMap(activityId => (activityId ? activityService.get(activityId) : of(undefined))),
        first()
      )
      .subscribe(activity => {
        this.mode = activity ? 'edit' : 'create';
        this.editedActivity = activity;

        if (activity) {
          const formValue = {
            title: activity.title,
            description: activity.description,
            date: activity.date
          };

          this.form.setValue(formValue);
        }
      });
  }

  save() {
    if (this.form.invalid) {
      return;
    }

    const formValue = this.form.getRawValue();
    const command: ActivityCommand = {
      title: formValue.title,
      description: formValue.description,
      date: formValue.date!
    };
    const result$: Observable<unknown> =
      this.mode === 'create'
        ? this.activityService.create(command)
        : this.activityService.update(this.editedActivity!.id, command);
    result$.pipe(this.saving.spinUntilFinalization()).subscribe(() => {
      this.router.navigate(['/activities']);
      this.toastService.success(this.mode === 'create' ? 'Activité crééé' : 'Activité modifiée');
    });
  }
}