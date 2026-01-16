import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NasaPictureTestComponent } from './nasa-picture-test.component';

describe('NasaPictureTestComponent', () => {
  let component: NasaPictureTestComponent;
  let fixture: ComponentFixture<NasaPictureTestComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NasaPictureTestComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NasaPictureTestComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});