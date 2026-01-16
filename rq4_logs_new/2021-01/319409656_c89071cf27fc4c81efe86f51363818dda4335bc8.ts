import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PubReqDetailComponent } from './pub-req-detail.component';

describe('PubReqDetailComponent', () => {
  let component: PubReqDetailComponent;
  let fixture: ComponentFixture<PubReqDetailComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ PubReqDetailComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PubReqDetailComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});