import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ChonGheComponent } from './chon-ghe.component';

describe('ChonGheComponent', () => {
  let component: ChonGheComponent;
  let fixture: ComponentFixture<ChonGheComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ChonGheComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ChonGheComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});