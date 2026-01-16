import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ListItemColComponent } from './list-item-col.component';

describe('ListItemColComponent', () => {
  let component: ListItemColComponent;
  let fixture: ComponentFixture<ListItemColComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ListItemColComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ListItemColComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});