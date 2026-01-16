import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CoutriesListComponent } from './coutries-list.component';

describe('CoutriesListComponent', () => {
  let component: CoutriesListComponent;
  let fixture: ComponentFixture<CoutriesListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ CoutriesListComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(CoutriesListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});