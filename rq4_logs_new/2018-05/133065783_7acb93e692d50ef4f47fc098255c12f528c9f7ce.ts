import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { HeaderJessComponent } from './header-jess.component';

describe('HeaderJessComponent', () => {
  let component: HeaderJessComponent;
  let fixture: ComponentFixture<HeaderJessComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ HeaderJessComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(HeaderJessComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});