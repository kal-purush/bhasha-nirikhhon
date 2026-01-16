import { ComponentFixture, TestBed } from '@angular/core/testing';

import { OrgWineComponent } from './org-wine.component';

describe('OrgWineComponent', () => {
  let component: OrgWineComponent;
  let fixture: ComponentFixture<OrgWineComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [OrgWineComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(OrgWineComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});