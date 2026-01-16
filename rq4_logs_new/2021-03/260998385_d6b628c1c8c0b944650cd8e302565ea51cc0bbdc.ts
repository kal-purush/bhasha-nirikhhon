import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { BooleanRendererForAgGridComponent } from './boolean-renderer-for-ag-grid.component';

describe('BooleanRendererForAgGridComponent', () => {
  let component: BooleanRendererForAgGridComponent;
  let fixture: ComponentFixture<BooleanRendererForAgGridComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ BooleanRendererForAgGridComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(BooleanRendererForAgGridComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});