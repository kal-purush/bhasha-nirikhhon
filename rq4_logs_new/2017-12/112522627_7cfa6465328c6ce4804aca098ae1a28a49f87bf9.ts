import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ClubAmComponent } from './club-am.component';

describe('ClubAmComponent', () => {
  let component: ClubAmComponent;
  let fixture: ComponentFixture<ClubAmComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ClubAmComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ClubAmComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});