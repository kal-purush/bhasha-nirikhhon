import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { LifegroupComponent } from './lifegroup.component';

describe('LifegroupComponent', () => {
  let component: LifegroupComponent;
  let fixture: ComponentFixture<LifegroupComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ LifegroupComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(LifegroupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});