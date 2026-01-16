import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NonNullableFormBuilder, ReactiveFormsModule, Validators } from '@angular/forms';
import {
  at,
  fileArrowUp,
  infoCircleFill,
  phone,
  telephone,
  whatsapp,
  xSquare
} from '../../bootstrap-icons/bootstrap-icons';
import { Spinner } from '../../shared/spinner';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { filter, first, switchMap } from 'rxjs';
import { Contact, ContactCommand, ContactService } from '../../shared/contact.service';
import { PageTitleDirective } from '../../page-title/page-title.directive';
import { ValidationErrorsComponent } from 'ngx-valdemort';
import { IconDirective } from '../../icon/icon.directive';
import { LoadingSpinnerComponent } from '../../loading-spinner/loading-spinner.component';
import { ToastService } from '../../toast/toast.service';
import { FormControlValidationDirective } from '../../validation/form-control-validation.directive';
import { CurrentUser, CurrentUserService } from '../../current-user.service';

@Component({
  selector: 'ms-my-contact-edition',
  standalone: true,
  imports: [
    CommonModule,
    PageTitleDirective,
    ReactiveFormsModule,
    ValidationErrorsComponent,
    FormControlValidationDirective,
    IconDirective,
    LoadingSpinnerComponent,
    RouterLink
  ],
  templateUrl: './my-contact-edition.component.html',
  styleUrls: ['./my-contact-edition.component.scss']
})
export class MyContactEditionComponent {
  form = this.fb.group({
    email: ['', Validators.email],
    phone: '',
    mobile: '',
    whatsapp: ''
  });
  mode: 'create' | 'edit' | null = null;
  editedContact?: Contact;
  icons = {
    email: at,
    phone: telephone,
    mobile: phone,
    whatsapp: whatsapp,
    save: fileArrowUp,
    cancel: xSquare,
    info: infoCircleFill
  };
  saving = new Spinner();

  constructor(
    route: ActivatedRoute,
    private router: Router,
    private contactService: ContactService,
    private fb: NonNullableFormBuilder,
    private toastService: ToastService,
    private currentUserService: CurrentUserService
  ) {
    currentUserService
      .getCurrentUser()
      .pipe(
        filter((user: CurrentUser | null): user is CurrentUser => !!user),
        switchMap(user => contactService.findByName(user.user.displayName!)),
        first()
      )
      .subscribe(contact => {
        this.mode = contact ? 'edit' : 'create';
        this.editedContact = contact;

        if (contact) {
          const formValue = {
            email: contact.email ?? '',
            phone: contact.phone ?? '',
            mobile: contact.mobile ?? '',
            whatsapp: contact.whatsapp ?? ''
          };

          this.form.setValue(formValue);
        }
      });
  }

  save() {
    if (this.form.invalid) {
      return;
    }

    const partialCommand: Omit<ContactCommand, 'name'> = this.form.getRawValue();
    this.currentUserService
      .getCurrentUser()
      .pipe(
        filter((user: CurrentUser | null): user is CurrentUser => !!user),
        first(),
        switchMap(user => {
          const command: ContactCommand = { ...partialCommand, name: user.user.displayName! };
          return this.mode === 'create'
            ? this.contactService.create(command)
            : this.contactService.update(this.editedContact!.id, command);
        }),
        this.saving.spinUntilFinalization()
      )
      .subscribe(() => {
        this.router.navigate(['/contacts']);
        this.toastService.success('Coordonnées enregistrées');
      });
  }
}