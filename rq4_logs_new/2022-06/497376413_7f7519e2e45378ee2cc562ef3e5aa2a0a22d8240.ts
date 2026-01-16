import { ComponentFixture, TestBed } from '@angular/core/testing';

import { StorageBookComponent } from './storage-book.component';

describe('StorageBookComponent', () => {
  let component: StorageBookComponent;
  let fixture: ComponentFixture<StorageBookComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ StorageBookComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(StorageBookComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});