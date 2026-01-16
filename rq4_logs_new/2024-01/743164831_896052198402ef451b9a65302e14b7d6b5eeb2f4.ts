import { Component, OnInit } from '@angular/core';
import { FormGroup, FormBuilder, Validators, FormControl } from '@angular/forms';

@Component({
  selector: 'message',
  templateUrl: './message.component.html',
  styleUrl: './message.component.css'
})
export class MessageComponent implements OnInit {
  messageForm: FormGroup = new FormGroup({
    messageBody: new FormControl<string>(''),
    emailRecipient: new FormControl<string>(''),
  });

  constructor(private fb: FormBuilder) {}

  ngOnInit(): void {
    this.initClearForm();
  }

  onSubmit() {
    console.log(this.messageForm.value);
  }

  clearAllFields(): void {
    this.initClearForm();
  }

  private initClearForm(): void {
    this.messageForm = this.fb.group({
      messageBody: ['', [Validators.required, Validators.minLength(3)]],
      emailRecipient: ['', Validators.email],
    });
  }
}