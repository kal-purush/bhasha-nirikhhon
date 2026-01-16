import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DatabaseHomeComponent } from './database-home.component';

describe('DatabaseHomeComponent', () => {
  let component: DatabaseHomeComponent;
  let fixture: ComponentFixture<DatabaseHomeComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DatabaseHomeComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DatabaseHomeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});