import { ComponentFixture, TestBed } from '@angular/core/testing';

import { FaceitComponent } from './faceit.component';

describe('FaceitComponent', () => {
  let component: FaceitComponent;
  let fixture: ComponentFixture<FaceitComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [FaceitComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(FaceitComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});