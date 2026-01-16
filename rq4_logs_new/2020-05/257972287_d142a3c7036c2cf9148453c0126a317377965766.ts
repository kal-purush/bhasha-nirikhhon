import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MisLikesComponent } from './mis-likes.component';

describe('MisLikesComponent', () => {
  let component: MisLikesComponent;
  let fixture: ComponentFixture<MisLikesComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MisLikesComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MisLikesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});