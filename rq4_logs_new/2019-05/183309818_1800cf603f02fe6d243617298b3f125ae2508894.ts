import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MessageDoneProductComponent } from './message-done-product.component';

describe('MessageDoneProductComponent', () => {
  let component: MessageDoneProductComponent;
  let fixture: ComponentFixture<MessageDoneProductComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MessageDoneProductComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MessageDoneProductComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});