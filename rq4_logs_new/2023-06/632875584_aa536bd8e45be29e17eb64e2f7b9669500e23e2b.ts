import { ComponentFixture, TestBed } from '@angular/core/testing';

import { VisitingProfilComponent } from './visiting-profil.component';

describe('VisitingProfilComponent', () => {
  let component: VisitingProfilComponent;
  let fixture: ComponentFixture<VisitingProfilComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ VisitingProfilComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(VisitingProfilComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});