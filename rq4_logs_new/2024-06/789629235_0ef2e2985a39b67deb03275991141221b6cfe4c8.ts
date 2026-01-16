import { ComponentFixture, TestBed } from '@angular/core/testing';

import { LecMainPageComponent } from './lec-main-page.component';

describe('LecMainPageComponent', () => {
  let component: LecMainPageComponent;
  let fixture: ComponentFixture<LecMainPageComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [LecMainPageComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(LecMainPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});