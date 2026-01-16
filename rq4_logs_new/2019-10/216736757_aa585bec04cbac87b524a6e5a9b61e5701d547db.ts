import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { KjhMainComponent } from './kjh-main.component';

describe('KjhMainComponent', () => {
  let component: KjhMainComponent;
  let fixture: ComponentFixture<KjhMainComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ KjhMainComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(KjhMainComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});