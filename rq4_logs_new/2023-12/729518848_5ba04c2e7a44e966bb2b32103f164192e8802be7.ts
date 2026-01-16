import { Component, inject } from '@angular/core';
import { ToastService } from '../../services/toasts.service';
import { NgTemplateOutlet } from '@angular/common';
import { NgbToastModule } from '@ng-bootstrap/ng-bootstrap';
@Component({
  selector: 'app-toasts',
  standalone: true,
  imports: [NgbToastModule, NgTemplateOutlet],
  templateUrl: './toasts-container.component.html',
  styleUrl: './toasts-container.component.css',
})
export class ToastsContainerComponent {
  constructor(public toastService: ToastService) {}
}