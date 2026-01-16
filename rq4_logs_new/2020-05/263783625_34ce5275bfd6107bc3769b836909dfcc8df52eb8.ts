import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';

@Component({
  selector: 'app-form-component',
  template: `
    <div>
      <h1>Angular Form Page</h1>
      <button (click)="handleShowTemplateForm()">Show Template Form</button>
      <button (click)="handleShowReactiveForm()">Show Reactive Form</button>
      <div>
        <router-outlet></router-outlet>
      </div>
    </div>`,
  styles: []
})
export class AppFormComponent implements OnInit {

  handleShowTemplateForm() {
    this.router.navigate(['template'], { relativeTo: this.route });
  }

  handleShowReactiveForm() {
    this.router.navigate(['reactive'], { relativeTo: this.route });
  }

  constructor(private router: Router, private route: ActivatedRoute) { }
  ngOnInit() { }
}