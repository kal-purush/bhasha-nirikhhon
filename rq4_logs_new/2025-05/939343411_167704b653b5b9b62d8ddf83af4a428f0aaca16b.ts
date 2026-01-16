import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DeleteMessagesMemberComponent } from './delete-messages-member.component';

describe('DeleteMessagesMemberComponent', () => {
  let component: DeleteMessagesMemberComponent;
  let fixture: ComponentFixture<DeleteMessagesMemberComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [DeleteMessagesMemberComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(DeleteMessagesMemberComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});