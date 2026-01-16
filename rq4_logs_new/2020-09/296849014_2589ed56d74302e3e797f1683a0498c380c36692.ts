import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ListedElementComponent } from './listed-element.component';

describe('ListedElementComponent', () => {
  let component: ListedElementComponent;
  let fixture: ComponentFixture<ListedElementComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ListedElementComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ListedElementComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});