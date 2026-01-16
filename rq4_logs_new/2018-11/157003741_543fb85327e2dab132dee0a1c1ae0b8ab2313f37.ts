import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { CreateRaidComponent } from './create-raid.component';

describe('CreateRaidComponent', () => {
  let component: CreateRaidComponent;
  let fixture: ComponentFixture<CreateRaidComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ CreateRaidComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(CreateRaidComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});