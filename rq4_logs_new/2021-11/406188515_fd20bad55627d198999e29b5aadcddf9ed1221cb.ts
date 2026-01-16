import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RecognizeBlightComponent } from './recognize-blight.component';

describe('RecognizeBlightComponent', () => {
  let component: RecognizeBlightComponent;
  let fixture: ComponentFixture<RecognizeBlightComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ RecognizeBlightComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RecognizeBlightComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});