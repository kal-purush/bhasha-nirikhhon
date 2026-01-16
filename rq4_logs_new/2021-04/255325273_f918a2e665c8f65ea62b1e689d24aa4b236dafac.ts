import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { SectionDesignComponent } from './section-design.component';

describe('SectionDesignComponent', () => {
  let component: SectionDesignComponent;
  let fixture: ComponentFixture<SectionDesignComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ SectionDesignComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(SectionDesignComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});