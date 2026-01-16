import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SitterloginComponent } from './sitterlogin.component';

describe('SitterloginComponent', () => {
  let component: SitterloginComponent;
  let fixture: ComponentFixture<SitterloginComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SitterloginComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SitterloginComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should be created', () => {
    expect(component).toBeTruthy();
  });
});