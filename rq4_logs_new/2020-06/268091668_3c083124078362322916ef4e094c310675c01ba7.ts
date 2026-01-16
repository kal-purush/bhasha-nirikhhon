import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { MakeLineComponent } from './make-line.component';

describe('MakeLineComponent', () => {
  let component: MakeLineComponent;
  let fixture: ComponentFixture<MakeLineComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MakeLineComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MakeLineComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});