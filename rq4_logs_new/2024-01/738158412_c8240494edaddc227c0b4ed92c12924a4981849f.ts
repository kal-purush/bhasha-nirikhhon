import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ReceptionPageComponent } from './reception-page.component';

describe('ReceptionPageComponent', () => {
  let component: ReceptionPageComponent;
  let fixture: ComponentFixture<ReceptionPageComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ReceptionPageComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(ReceptionPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});