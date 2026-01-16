import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RecordingActEditionComponent } from './recording-act-edition.component';

describe('RecordingActEditionComponent', () => {
  let component: RecordingActEditionComponent;
  let fixture: ComponentFixture<RecordingActEditionComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ RecordingActEditionComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RecordingActEditionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});