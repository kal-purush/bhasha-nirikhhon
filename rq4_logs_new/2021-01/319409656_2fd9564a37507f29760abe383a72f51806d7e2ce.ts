import { ComponentFixture, TestBed } from '@angular/core/testing';

import { UnpublishedBookComponent } from './unpublished-book.component';

describe('UnpublishedBookComponent', () => {
  let component: UnpublishedBookComponent;
  let fixture: ComponentFixture<UnpublishedBookComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ UnpublishedBookComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UnpublishedBookComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});