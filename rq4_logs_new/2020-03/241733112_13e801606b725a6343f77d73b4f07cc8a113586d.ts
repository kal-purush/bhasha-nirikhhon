import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CodeRequestComponent } from './code-request.component';

describe('CodeRequestComponent', () => {
  let component: CodeRequestComponent;
  let fixture: ComponentFixture<CodeRequestComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CodeRequestComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CodeRequestComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});