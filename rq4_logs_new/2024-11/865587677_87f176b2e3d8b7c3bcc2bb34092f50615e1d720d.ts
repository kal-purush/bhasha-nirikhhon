import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ResponseDisplayComponent } from './response-display.component';

describe('ResponseDisplayComponent', () => {
  let component: ResponseDisplayComponent;
  let fixture: ComponentFixture<ResponseDisplayComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ResponseDisplayComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(ResponseDisplayComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});