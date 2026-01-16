import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DeleteImageModalComponent } from './delete-image-modal.component';

describe('DeleteImageModalComponent', () => {
  let component: DeleteImageModalComponent;
  let fixture: ComponentFixture<DeleteImageModalComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [DeleteImageModalComponent],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DeleteImageModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});