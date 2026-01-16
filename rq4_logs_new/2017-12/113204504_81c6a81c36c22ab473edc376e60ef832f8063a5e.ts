import {
  Component,
  OnInit,
  ViewChild
} from '@angular/core';
import {
  FormControl,
  FormGroup,
  Validators
} from '@angular/forms';

import { Matcher } from '../../../../extension/matcher.validator';
import { NotificationComponent } from '../../../../../shared/module/notification';
import { FormBaseComponent } from '../../../../../shared/module/form-base';

@Component({
  selector: 'app-change-password',
  templateUrl: './change-password.component.html',
  styleUrls: ['./change-password.component.scss']
})
export class ChangePasswordComponent extends FormBaseComponent implements OnInit {

  public form: FormGroup;
  public controlConfig = {
    password: new FormControl('', [Validators.required]),
    newPassword: new FormControl('', [Validators.required]),
    confirmNewPassword: new FormControl('', [Matcher('newPassword'), Validators.required]),
  };
  public validatorMessages = {
    password: {
      required: 'Vui lòng nhập mật khẩu hiện tại'
    },
    newPassword: {
      required: 'Vui lòng nhập mật khẩu mới'
    },
    confirmNewPassword: {
      required: 'Vui lòng nhập xác nhận mật khẩu',
      Matcher: 'Mật khẩu không trùng khớp với nhau',
    },
  };
  public formErrors = {
    password: '',
    newPassword: '',
    confirmNewPassword: ''
  };

  public configForm = {
    updateOn: 'change'
  };

  @ViewChild('notification') notification: NotificationComponent;

  constructor() {
    super();
  }

  ngOnInit() {
    super.ngOnInit();
    this.form.valueChanges.subscribe(() => this.validatorForm());
  }

  public changePassword(value) {
    this.validatorForm(true);
    const {password, newPassword, confirmNewPassword} = value;
    if (this.form.valid) {

    }
  }

}