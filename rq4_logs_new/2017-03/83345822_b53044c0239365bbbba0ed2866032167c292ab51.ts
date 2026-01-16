/* tslint:disable:no-unused-variable */

import { TestBed, async } from '@angular/core/testing';
import {JudgeComponent} from "./judge.component";

describe('JudgeComponent', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [
        JudgeComponent
      ],
    });
    TestBed.compileComponents();
  });

  // it('should create the app', async(() => {
  //   const fixture = TestBed.createComponent(JudgeComponent);
  //   const app = fixture.debugElement.componentInstance;
  //   expect(app).toBeTruthy();
  // }));

  // it(`should have as title 'app works!'`, async(() => {
  //   const fixture = TestBed.createComponent(JudgeComponent);
  //   const app = fixture.debugElement.componentInstance;
  //   expect(app.title).toEqual('app works!');
  // }));

  // it('should render title in a h1 tag', async(() => {
  //   const fixture = TestBed.createComponent(JudgeComponent);
  //   fixture.detectChanges();
  //   const compiled = fixture.debugElement.nativeElement;
  //   expect(compiled.querySelector('h1').textContent).toContain('app works!');
  // }));
});