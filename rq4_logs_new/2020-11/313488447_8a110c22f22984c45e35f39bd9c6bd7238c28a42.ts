import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ImgWordComponent } from './img-word.component';

describe('ImgWordComponent', () => {
  let component: ImgWordComponent;
  let fixture: ComponentFixture<ImgWordComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ImgWordComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ImgWordComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});