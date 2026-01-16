import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { AaratiComponent } from './aarati.component';

describe('AaratiComponent', () => {
  let component: AaratiComponent;
  let fixture: ComponentFixture<AaratiComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ AaratiComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AaratiComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});