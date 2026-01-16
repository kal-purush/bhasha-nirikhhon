import { ComponentFixture, TestBed } from '@angular/core/testing';

import { UpdateTablesComponent } from './update-tables.component';

describe('UpdateTablesComponent', () => {
  let component: UpdateTablesComponent;
  let fixture: ComponentFixture<UpdateTablesComponent>;

  beforeEach(() => {
    TestBed.configureTestingModule({
      declarations: [UpdateTablesComponent]
    });
    fixture = TestBed.createComponent(UpdateTablesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});