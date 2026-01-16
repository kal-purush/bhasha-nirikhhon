import { Component, OnInit } from '@angular/core';
import { NonNullableFormBuilder, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { BehaviorSubject, NEVER, catchError, map, switchMap, tap } from 'rxjs';
import { AbstractFormComponent } from 'src/app/components/abstract-form/abstract-form.component';
import { AppError } from 'src/app/core/models/app-error/app-error';
import { RubricBase } from 'src/app/core/models/rubric/rubric';
import { Destroyable, takeUntilDestroy } from 'src/app/core/utils/destroyable';
import { FlatControlsOf } from 'src/app/core/utils/forms/controls';
import { NotificationService } from 'src/app/services/notification.service';
import { RubricService } from 'src/app/services/rubric.service';

type FormControls = FlatControlsOf<RubricBase>;

@Destroyable()
@Component({
  selector: 'app-rubric-edit-form',
  templateUrl: './rubric-edit-form.component.html',
  styleUrls: ['./rubric-edit-form.component.css']
})
export class RubricEditFormComponent
  extends AbstractFormComponent<RubricBase>
  implements OnInit
{

  /** Page title. */
  protected readonly title$ = new BehaviorSubject<string | null>(null);

  private readonly id$ = new BehaviorSubject<string | null>(null);

  protected readonly form = this.fb.group<FormControls>({
    name: this.fb.control('', Validators.required),
  });

  public constructor(
    private readonly fb: NonNullableFormBuilder,
    private readonly router: Router,
    private readonly route: ActivatedRoute,
    private readonly notification: NotificationService,
    private readonly rubricService: RubricService,
  ) {
    super();
  }

  public ngOnInit(): void {
    this.route.params.pipe(
      switchMap(params => {
        const id = params['id'];

        if (id === undefined) {
          this.title$.next('New rubric');
          return NEVER;
        }

        return this.rubricService.getRubrics()
          .pipe(
            map(rubrics => {
              this.title$.next('Edit Blog');
              this.id$.next(id);
              const rubric = rubrics.find(e => e.id === Number(id)) ?? { id: -1, name: '' };
              this.form.patchValue({
                name: rubric?.name ?? ''
              })
            }),
            catchError(() => this.router.navigate(['/'])),
          )
      }),
      takeUntilDestroy(this),
    )
      .subscribe()
  }

  protected onSubmit(): void {
    this.form.markAllAsTouched();

    if (this.form.invalid) {
      return;
    }

    const formData = this.form.getRawValue();

    const rubric: RubricBase = {
      name: formData.name,
    }

    this.id$.pipe(
      switchMap(id => {
        if (!id) {
          return this.rubricService.createRubric({
            ...rubric,
            id: Number(id),
          });
        }

        return this.rubricService.editRubric({
          ...rubric,
          id: Number(id),
        })
      }),
      tap(() => {
        this.router.navigate(['/', 'admin',  'rubrics']);
      }),
      catchError((err: AppError) => {
        this.notification.showAppError(err);

        return NEVER;
      }),
      takeUntilDestroy(this),
    ).subscribe();
  }
}