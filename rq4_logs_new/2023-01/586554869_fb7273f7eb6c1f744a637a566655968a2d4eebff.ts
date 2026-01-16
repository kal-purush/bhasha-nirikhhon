import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NonNullableFormBuilder, ReactiveFormsModule, Validators } from '@angular/forms';
import {
  at,
  fileArrowUp,
  phone,
  telephone,
  whatsapp,
  xSquare
} from '../../bootstrap-icons/bootstrap-icons';
import { Spinner } from '../../shared/spinner';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';
import { first, map, Observable, of, switchMap } from 'rxjs';
import { Contact, ContactCommand, ContactService } from '../../shared/contact.service';
import { PageTitleDirective } from '../../page-title/page-title.directive';
import { ValidationErrorsComponent } from 'ngx-valdemort';
import { IconDirective } from '../../icon/icon.directive';
import { LoadingSpinnerComponent } from '../../loading-spinner/loading-spinner.component';

@Component({
  selector: 'ms-contact-edition',
  standalone: true,
  imports: [
    CommonModule,
    PageTitleDirective,
    ReactiveFormsModule,
    ValidationErrorsComponent,
    IconDirective,
    LoadingSpinnerComponent,
    RouterLink
  ],
  templateUrl: './contact-edition.component.html',
  styleUrls: ['./contact-edition.component.scss']
})
export class ContactEditionComponent {
  form = this.fb.group({
    name: ['', Validators.required],
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
    cancel: xSquare
  };
  saving = new Spinner();

  constructor(
    route: ActivatedRoute,
    private router: Router,
    private contactService: ContactService,
    private fb: NonNullableFormBuilder
  ) {
    route.paramMap
      .pipe(
        map(paramMap => paramMap.get('contactId')),
        switchMap(contactId => (contactId ? contactService.get(contactId) : of(undefined))),
        first()
      )
      .subscribe(contact => {
        this.mode = contact ? 'edit' : 'create';
        this.editedContact = contact;

        if (contact) {
          const formValue = {
            name: contact.name,
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

    const command: ContactCommand = this.form.getRawValue();
    const result$: Observable<unknown> =
      this.mode === 'create'
        ? this.contactService.create(command)
        : this.contactService.update(this.editedContact!.id, command);
    result$.pipe(this.saving.spinUntilFinalization()).subscribe(() => {
      this.router.navigate(['/contacts']);
    });
  }
}