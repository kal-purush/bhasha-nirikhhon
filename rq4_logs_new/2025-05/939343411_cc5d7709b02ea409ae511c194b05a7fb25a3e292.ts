import { ComponentFixture, TestBed } from '@angular/core/testing';

import { EditDeleteChannelComponent } from './edit-delete-channel.component';

describe('EditDeleteChannelComponent', () => {
  let component: EditDeleteChannelComponent;
  let fixture: ComponentFixture<EditDeleteChannelComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [EditDeleteChannelComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(EditDeleteChannelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});