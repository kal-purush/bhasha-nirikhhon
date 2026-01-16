import { ComponentFixture, TestBed } from '@angular/core/testing';

import { UpdatenoticeComponent } from './updatenotice.component';

describe('UpdatenoticeComponent', () => {
  let component: UpdatenoticeComponent;
  let fixture: ComponentFixture<UpdatenoticeComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ UpdatenoticeComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UpdatenoticeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});