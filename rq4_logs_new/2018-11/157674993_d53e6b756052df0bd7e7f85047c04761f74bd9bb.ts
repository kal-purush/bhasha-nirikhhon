import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { OffsiteComponent } from './offsite.component';

describe('OffsiteComponent', () => {
  let component: OffsiteComponent;
  let fixture: ComponentFixture<OffsiteComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ OffsiteComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(OffsiteComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});