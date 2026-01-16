import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ZusammenfassungComponent } from './zusammenfassung.component';

describe('ZusammenfassungComponent', () => {
  let component: ZusammenfassungComponent;
  let fixture: ComponentFixture<ZusammenfassungComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ZusammenfassungComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ZusammenfassungComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});