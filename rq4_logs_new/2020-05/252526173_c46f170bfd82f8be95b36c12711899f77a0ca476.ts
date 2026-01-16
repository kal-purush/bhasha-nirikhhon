import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ImagineHomeComponent } from './imagine-home.component';

describe('ImagineHomeComponent', () => {
  let component: ImagineHomeComponent;
  let fixture: ComponentFixture<ImagineHomeComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ImagineHomeComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ImagineHomeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});