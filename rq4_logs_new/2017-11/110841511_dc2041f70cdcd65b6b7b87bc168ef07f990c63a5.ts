import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MainContentComponentComponent } from './main-content-component.component';

describe('MainContentComponentComponent', () => {
  let component: MainContentComponentComponent;
  let fixture: ComponentFixture<MainContentComponentComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MainContentComponentComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MainContentComponentComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});