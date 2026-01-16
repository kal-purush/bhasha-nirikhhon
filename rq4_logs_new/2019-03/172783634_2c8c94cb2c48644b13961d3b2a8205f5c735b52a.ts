import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SjsAdditionComponent } from './sjs-addition.component';

describe('SjsAdditionComponent', () => {
  let component: SjsAdditionComponent;
  let fixture: ComponentFixture<SjsAdditionComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SjsAdditionComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SjsAdditionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});